package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"tgntf/internal/app"
	"tgntf/internal/config"
)

func main() { os.Exit(run()) }

func run() int {
	check := flag.Bool("check-config", false, "validate configuration without calling external APIs")
	health := flag.Bool("healthcheck", false, "check local readiness (used by Docker)")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *health {
		addr := strings.TrimSpace(os.Getenv("HEALTH_ADDR"))
		if addr == "" {
			addr = "127.0.0.1:8080"
		}
		if strings.HasPrefix(addr, "0.0.0.0:") || strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1:" + addr[strings.LastIndex(addr, ":")+1:]
		}
		client := &http.Client{Timeout: 3 * time.Second}
		res, err := client.Get("http://" + addr + "/readyz")
		if err != nil {
			return 1
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return 1
		}
		return 0
	}
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("Configuration invalid", "error", err)
		return 1
	}
	if *check {
		fmt.Println("Configuration valid")
		return 0
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg, logger); err != nil {
		logger.Error("Notification bot stopped", "error", err)
		return 1
	}
	logger.Info("Notification bot stopped")
	return 0
}
