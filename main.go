package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// version is overridden at build time via -ldflags '-X main.version=<sha>'.
var version = "dev"

func main() {
	port := envOrDefault("PORT", "8080")
	kubeconfig := os.Getenv("KUBECONFIG")

	// JSON logs so kubectl logs / log aggregators can parse structured fields.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Build a Kubernetes client config. When KUBECONFIG is set we use that
	// (local dev / CI); inside a pod the service-account token is used instead.
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("build kubeconfig", "err", err)
		os.Exit(1)
	}

	// The dynamic client works with any resource type via GroupVersionResource,
	// so we don't need generated typed clients for each CRD.
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		slog.Error("build dynamic client", "err", err)
		os.Exit(1)
	}

	// ctx is cancelled on SIGINT / SIGTERM, which unblocks the shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start one watch goroutine per resource kind (XRs, managed resources, pods).
	w := newWatcher(dynClient)
	go w.run(ctx)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		slog.Info("platform-exporter listening", "port", port, "version", version)
		// ListenAndServe always returns a non-nil error; ErrServerClosed is
		// expected during graceful shutdown and is not treated as a failure.
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	// Block until a signal arrives, then drain in-flight HTTP requests.
	<-ctx.Done()
	slog.Info("shutting down")
	_ = srv.Shutdown(context.Background())
}

// envOrDefault returns the value of the environment variable key, or def if
// the variable is unset or empty.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// podGVR returns the GroupVersionResource for core/v1 Pods.
func podGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
}
