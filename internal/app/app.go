package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"tgntf/internal/binance"
	"tgntf/internal/config"
	"tgntf/internal/store"
	"tgntf/internal/telegram"
	"tgntf/internal/trade"
)

type Sender interface {
	Send(context.Context, string) error
}

func Run(parent context.Context, cfg config.Config, logger *slog.Logger) error {
	db, err := store.Open(cfg.StatePath)
	if err != nil {
		return fmt.Errorf("open notification store: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var mu sync.RWMutex
	ready := map[string]bool{}
	if cfg.Spot {
		ready["spot"] = false
	}
	if cfg.Futures {
		ready["futures"] = false
	}
	notificationOK := true
	setReady := func(market string, value bool) { mu.Lock(); ready[market] = value; mu.Unlock() }
	setDelivery := func(value bool) { mu.Lock(); notificationOK = value; mu.Unlock() }
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		ok := notificationOK
		for _, connected := range ready {
			ok = ok && connected
		}
		if !ok {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		return fmt.Errorf("listen for health checks: %w", err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	errorsCh := make(chan error, 4)
	var wg sync.WaitGroup
	launch := func(fn func() error) { wg.Add(1); go func() { defer wg.Done(); errorsCh <- fn() }() }
	launch(func() error {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	notify := make(chan struct{}, 1)
	handler := func(fill trade.Fill) error {
		if cfg.ExcludesSymbol(fill.Symbol) {
			return nil
		}
		added, err := db.Enqueue(fill)
		if err != nil {
			return err
		}
		if added {
			select {
			case notify <- struct{}{}:
			default:
			}
		}
		return nil
	}
	client := binance.New(cfg, logger, setReady)
	if cfg.Spot {
		launch(func() error { return client.Run(ctx, "spot", handler) })
	}
	if cfg.Futures {
		launch(func() error { return client.Run(ctx, "futures", handler) })
	}
	launch(func() error {
		return Deliver(ctx, db, telegram.New(cfg.TelegramToken, cfg.ChatID, cfg.TelegramAPI), cfg, logger, notify, setDelivery)
	})
	logger.Info("Binance notification bot started", "spot", cfg.Spot, "futures", cfg.Futures, "partial_fills", cfg.PartialFills)
	select {
	case <-parent.Done():
		err = nil
	case err = <-errorsCh:
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	_ = server.Shutdown(shutdownCtx)
	wg.Wait()
	return err
}

// Deliver sends one summary for each completed order, retaining open orders on disk.
func Deliver(ctx context.Context, db *store.Store, sender Sender, cfg config.Config, logger *slog.Logger, notify <-chan struct{}, state func(bool)) error {
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	prune := time.NewTicker(time.Hour)
	defer prune.Stop()
	if err := db.Prune(time.Now()); err != nil {
		return err
	}
	if err := db.DiscardExcluded(cfg.ExcludesSymbol, time.Now()); err != nil {
		return fmt.Errorf("discard excluded notifications: %w", err)
	}
	backoff := time.Second
	for ctx.Err() == nil {
		item, err := db.NextOrder()
		if err != nil {
			return err
		}
		if item == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-notify:
			case <-poll.C:
			case now := <-prune.C:
				if err := db.Prune(now); err != nil {
					return err
				}
			}
			continue
		}
		fill := item.Order.Fill
		if cfg.ExcludesSymbol(fill.Symbol) || (!cfg.PartialFills && fill.Status != "FILLED") {
			if err := db.AckOrder(*item, time.Now()); err != nil {
				return fmt.Errorf("acknowledge skipped order: %w", err)
			}
			continue
		}
		err = sender.Send(ctx, item.Order.Message(cfg.Location))
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			state(false)
			var telegramErr *telegram.Error
			delay := backoff
			if errors.As(err, &telegramErr) {
				if telegramErr.Permanent {
					return err
				}
				delay = max(delay, telegramErr.RetryAfter)
			}
			logger.Warn("Telegram delivery failed; notification retained", "error", err, "retry_in", delay)
			if !wait(ctx, delay) {
				return nil
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		if err := db.AckOrder(*item, time.Now()); err != nil {
			return fmt.Errorf("acknowledge sent notification: %w", err)
		}
		state(true)
		backoff = time.Second
		logger.Info("Order summary delivered", "market", fill.Market, "symbol", fill.Symbol, "order_id", fill.OrderID, "trades", item.Order.TradeCount, "complete_records", item.Order.Complete())
		// Telegram recommends avoiding more than one message per second in one chat.
		if !wait(ctx, time.Second) {
			return nil
		}
		select {
		case now := <-prune.C:
			if err := db.Prune(now); err != nil {
				return err
			}
		default:
		}
	}
	return nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
