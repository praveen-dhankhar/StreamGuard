package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"streamguard/internal/auth"
	"streamguard/internal/config"
	"streamguard/internal/reconcile"
	"streamguard/internal/server"
)

func main() {
	configPath := os.Getenv("STREAMGUARD_CONFIG")
	if configPath == "" {
		configPath = "config.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("config_error=%v", err)
	}
	keys := auth.NewStore(cfg.Budget.DefaultPeriod)
	if err := keys.LoadKeysFile(cfg.Auth.KeysFile); err != nil {
		log.Fatalf("keys_error=%v", err)
	}
	srv := server.New(cfg, keys)
	runCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	reconcileRunner := reconcile.Runner{
		Job: reconcile.Job{
			Ledger:       srv.Ledger(),
			Calibration:  srv.Calibration(),
			TokenizerReg: srv.TokenizerRegistry(),
			ThresholdPct: cfg.Reconciliation.DriftThresholdPct,
			Fetcher:      reconcile.HTTPUsageFetcher{Client: &http.Client{Timeout: 5 * time.Second}},
		},
		Providers: cfg.Providers,
		Interval:  cfg.Reconciliation.Interval,
	}
	httpSrv := &http.Server{
		Addr:    envDefault("STREAMGUARD_ADDR", ":8080"),
		Handler: srv.Handler(),
	}

	go func() {
		log.Printf("streamguard_listen=%s", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen_error=%v", err)
		}
	}()
	go func() {
		ticker := time.NewTicker(reconcileRunner.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if _, err := reconcileRunner.RunOnce(runCtx); err != nil {
					log.Printf("reconcile_error=%v", err)
				}
			}
		}
	}()
	go keys.RunBudgetResetter(runCtx, time.Minute)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	stopBackground()
	srv.BeginShutdown(time.Duration(cfg.Shutdown.DrainTimeoutSeconds) * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Shutdown.DrainTimeoutSeconds)*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("forced_shutdown error=%v", err)
		_ = httpSrv.Close()
	}
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
