package main

import (
	"archive/tar"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// runWrapper is the entry point of a warm pod. It waits idle, and when the shim sends
// an activation it restores /data, writes the identity files, sets the environment
// the template asked for and replaces itself with the runtime.
func runWrapper(args []string) error {
	fs := flag.NewFlagSet("wrapper", flag.ExitOnError)
	listen := fs.String("listen", fmt.Sprintf(":%d", wrapperPort), "activation listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	activated := make(chan activation, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
		var act activation
		if r.Method != http.MethodPost || json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&act) != nil || len(act.Command) == 0 {
			http.Error(w, "POST an activation with a command", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 80*time.Second)
		defer cancel()
		if err := prepare(ctx, act); err != nil {
			slog.Error("prepare activation", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		select {
		case activated <- act:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "already activated", http.StatusConflict)
		}
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("wrapper server", "error", err)
			os.Exit(1)
		}
	}()
	act := <-activated
	// Let the response reach the shim before the process image is replaced.
	time.Sleep(300 * time.Millisecond)
	path, err := exec.LookPath(act.Command[0])
	if err != nil {
		return err
	}
	env := os.Environ()
	for k, v := range act.Env {
		env = append(env, k+"="+v)
	}
	return syscall.Exec(path, act.Command, env)
}

func prepare(ctx context.Context, act activation) error {
	if act.Restore != nil && act.DataDir != "" {
		if err := restoreData(ctx, *act.Restore, act.DataDir); err != nil {
			return err
		}
	}
	for name, content := range act.Files {
		clean := filepath.Clean(name)
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
			return fmt.Errorf("unsafe identity file %q", name)
		}
		target := filepath.Join(act.IdentityDir, clean)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func runInstall(args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	dest := fs.String("dest", wrapperPath, "where to copy this binary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	in, err := os.Open(self)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(*dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func runRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	url := fs.String("url", "", "snapshot URL")
	token := fs.String("token", "", "snapshot token")
	dest := fs.String("dest", "/data", "directory to restore into")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return restoreData(ctx, snapshotRef{URL: *url, Token: *token}, *dest)
}

func runSidecar(args []string) error {
	fs := flag.NewFlagSet("sidecar", flag.ExitOnError)
	src := fs.String("src", "/data", "directory to save")
	listen := fs.String("listen", fmt.Sprintf(":%d", sidecarPort), "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return (&http.Server{Addr: *listen, Handler: sidecarHandler(*src), ReadHeaderTimeout: 10 * time.Second}).ListenAndServe()
}

func runStore(args []string) error {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "listen address")
	dir := fs.String("dir", "/var/lib/ate-snapshots", "where snapshots live")
	secretFile := fs.String("secret-file", "/etc/ate-snapshot/secret", "shared secret")
	if err := fs.Parse(args); err != nil {
		return err
	}
	secret, err := os.ReadFile(*secretFile)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		return err
	}
	return (&http.Server{Addr: *listen, Handler: storeHandler(*dir, []byte(strings.TrimSpace(string(secret)))), ReadHeaderTimeout: 10 * time.Second}).ListenAndServe()
}

// runExtract unpacks a container image's filesystem into a directory. It stands in for a
// Substrate image volume: Fargate has none, so an init container fills an emptyDir instead.
func runExtract(args []string) error {
	fs := flag.NewFlagSet("extract", flag.ExitOnError)
	image := fs.String("image", "", "image reference")
	dest := fs.String("dest", "/mnt", "directory to unpack into")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ref, err := name.ParseReference(*image)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	img, err := remote.Image(ref, remote.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("pull %s: %w", *image, err)
	}
	rc := mutate.Extract(img)
	defer rc.Close()
	return untarEntries(tar.NewReader(rc), *dest, false)
}
