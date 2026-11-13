package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ranajahanzaib/zero-downtime-db-migrations/internal/app"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := app.ConfigFromEnv()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	srv, err := app.New(cfg)
	if err != nil {
		slog.Error("startup", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 10_000_000_000)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	slog.Info("http server starting", "addr", cfg.Addr, "version", cfg.Version, "read_mode", cfg.ReadMode)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http server", "error", err)
		os.Exit(1)
	}
}
