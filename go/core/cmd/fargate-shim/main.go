// Command fargate-shim serves the subset of the Agent Substrate (ate-api) Control
// API that kagent uses and runs every Actor as an AWS Fargate pod.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const stateMapName = "ate-shim-state"

func main() {
	if err := run(); err != nil {
		slog.Error("fargate-shim stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	grpcAddr := flag.String("grpc-addr", ":8443", "TLS listen address of the ate-api compatible server")
	routerAddr := flag.String("router-addr", ":8080", "listen address of the router (cleartext HTTP/2)")
	certFile := flag.String("tls-cert", "/etc/ate-shim/tls.crt", "server certificate")
	keyFile := flag.String("tls-key", "/etc/ate-shim/tls.key", "server private key")
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace that stores the shim state")
	actorCPU := flag.String("actor-cpu", "500m", "CPU request and limit of every actor pod")
	actorMemory := flag.String("actor-memory", "1Gi", "memory request and limit of every actor pod")
	actorSA := flag.String("actor-service-account", "", "service account of actor pods")
	runAsRoot := flag.Bool("actor-run-as-root", true, "run actor containers as root, like inside a Substrate sandbox")
	grace := flag.Duration("suspend-grace", 10*time.Minute, "how long a suspended actor keeps its pod")
	flag.Parse()
	if *namespace == "" {
		return fmt.Errorf("--namespace or POD_NAMESPACE is required")
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	srv := newServer(&k8sPods{client: client, settings: podSettings{CPU: *actorCPU, Memory: *actorMemory, ServiceAccount: *actorSA, RunAsRoot: *runAsRoot}},
		stateSaver(client, *namespace), config{SuspendGrace: *grace, ResumeWait: 25 * time.Second, RouterWait: 150 * time.Second})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cm, err := client.CoreV1().ConfigMaps(*namespace).Get(ctx, stateMapName, metav1.GetOptions{}); err == nil {
		if err := srv.load([]byte(cm.Data["state.json"])); err != nil {
			return fmt.Errorf("load state: %w", err)
		}
		slog.Info("state restored", "actors", len(srv.actors), "templates", len(srv.templates))
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	ateapipb.RegisterControlServer(grpcServer, srv)
	routerServer := &http.Server{Addr: *routerAddr, Handler: srv.routerHandler(), ReadHeaderTimeout: 10 * time.Second}

	errs := make(chan error, 2)
	go func() { errs <- grpcServer.Serve(lis) }()
	go func() { errs <- routerServer.ListenAndServe() }()
	go srv.runReaper(ctx, 15*time.Second)
	slog.Info("fargate-shim started", "grpc", *grpcAddr, "router", *routerAddr)

	select {
	case <-ctx.Done():
	case err := <-errs:
		return err
	}
	grpcServer.GracefulStop()
	return routerServer.Shutdown(context.Background())
}

func stateSaver(client kubernetes.Interface, namespace string) func([]byte) error {
	return func(raw []byte) error {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: stateMapName, Namespace: namespace},
			Data:       map[string]string{"state.json": string(raw)},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if apierrors.IsNotFound(err) {
			_, err = client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
		}
		return err
	}
}
