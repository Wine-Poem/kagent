package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const targetActorHeader = "ate-target-actor"

type targetKey struct{}

func parseTarget(value string) (ref, error) {
	atespace, name, ok := strings.Cut(value, "/")
	if !ok || atespace == "" || name == "" || strings.Contains(name, "/") {
		return ref{}, fmt.Errorf("%s must be atespace/actor-name, got %q", targetActorHeader, value)
	}
	return ref{atespace, name}, nil
}

// routerHandler stands in for the Atenet router: it resumes a suspended actor on
// demand, then forwards the (gRPC over cleartext HTTP/2) request to the actor's pod.
func (s *server) routerHandler() http.Handler {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(pr.In.Context().Value(targetKey{}).(*url.URL))
			pr.Out.Host = pr.In.Host
		},
		FlushInterval: -1,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
			},
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Warn("proxy error", "target", r.Header.Get(targetActorHeader), "error", err)
			grpcError(w, codes.Unavailable, "actor is unreachable: "+err.Error())
		},
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := parseTarget(r.Header.Get(targetActorHeader))
		if err != nil {
			grpcError(w, codes.InvalidArgument, err.Error())
			return
		}
		state, err := s.ensureRunning(r.Context(), key, s.cfg.RouterWait)
		if err != nil {
			grpcError(w, status.Code(err), err.Error())
			return
		}
		target := &url.URL{Scheme: "http", Host: net.JoinHostPort(state.IP, "80")}
		proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), targetKey{}, target)))
	})
	return h2c.NewHandler(handler, &http2.Server{})
}

// grpcError answers with a trailers-only gRPC response so gRPC clients see the code.
func grpcError(w http.ResponseWriter, code codes.Code, message string) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", fmt.Sprint(int(code)))
	w.Header().Set("Grpc-Message", url.PathEscape(message))
	w.WriteHeader(http.StatusOK)
}
