package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tgntf/internal/config"
	"tgntf/internal/store"
	"tgntf/internal/telegram"
	"tgntf/internal/trade"
)

type sendFunc func(context.Context, string) error

func (f sendFunc) Send(ctx context.Context, text string) error { return f(ctx, text) }

func completedFill() trade.Fill {
	return trade.Fill{Market: "spot", Symbol: "BTCUSDT", Side: "BUY", Status: "FILLED", OrderID: 1, TradeID: 2, Time: 1728973001334,
		Price: "10", Quantity: "1", QuoteQuantity: "10", CumulativeQuantity: "1", AveragePrice: "10"}
}

func TestDeliveryRetriesPersistedFill(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fill := completedFill()
	if _, err := db.Enqueue(fill); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	success := make(chan struct{})
	sender := sendFunc(func(ctx context.Context, text string) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary network failure")
		}
		close(success)
		return nil
	})
	result := make(chan error, 1)
	go func() {
		result <- Deliver(ctx, db, sender, config.Config{Location: time.UTC}, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}), func(bool) {})
	}()
	select {
	case <-success:
	case <-ctx.Done():
		t.Fatal("notification was not retried")
	}
	// Wait for the acknowledgement, which happens after Send returns.
	for {
		item, err := db.Next()
		if err != nil {
			t.Fatal(err)
		}
		if item == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("sent fill not acknowledged")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatal("unexpected retry count")
	}
	if added, err := db.Enqueue(fill); err != nil || added {
		t.Fatal("successful delivery did not preserve dedup marker")
	}
}

func TestPermanentFailurePreservesPending(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.Enqueue(completedFill())
	permanent := &telegram.Error{Code: 403, Permanent: true}
	err = Deliver(context.Background(), db, sendFunc(func(context.Context, string) error { return permanent }), config.Config{Location: time.UTC}, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}), func(bool) {})
	if !errors.Is(err, permanent) {
		t.Fatal("permanent error not returned")
	}
	if item, err := db.Next(); err != nil || item == nil {
		t.Fatal("failed notification lost")
	}
}

func TestCancelDuringRetryPreservesPending(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.Enqueue(completedFill())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = Deliver(ctx, db, sendFunc(func(context.Context, string) error { cancel(); return errors.New("network down") }), config.Config{Location: time.UTC}, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}), func(bool) {})
	if err != nil {
		t.Fatal(err)
	}
	if item, err := db.Next(); err != nil || item == nil {
		t.Fatal("shutdown dropped notification")
	}
}

func TestDeliverySkipsExcludedPersistedFills(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	excluded := []trade.Fill{
		{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 2},
		{Market: "futures", Symbol: "BTCUSDT", OrderID: 3, TradeID: 4},
	}
	allowed := completedFill()
	allowed.Symbol, allowed.OrderID, allowed.TradeID = "BTCUSDC", 5, 6
	for _, fill := range append(excluded, allowed) {
		if _, err := db.Enqueue(fill); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Apply the exclusion after restarting with a previously populated queue.
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := config.Config{Location: time.UTC, ExcludedSymbols: map[string]struct{}{"BTCUSDT": {}}}
	permanent := &telegram.Error{Code: 403, Permanent: true}
	attempts := 0
	sender := sendFunc(func(_ context.Context, message string) error {
		attempts++
		if strings.Contains(message, "BTCUSDT") || !strings.Contains(message, allowed.Symbol) {
			t.Errorf("unexpected notification: %s", message)
		}
		return permanent
	})
	err = Deliver(context.Background(), db, sender, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}), func(bool) {})
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatalf("expected only the allowed fill to reach Telegram: attempts=%d, error=%v", attempts, err)
	}
	if item, err := db.Next(); err != nil || item == nil || item.Fill.Key() != allowed.Key() {
		t.Fatalf("excluded fills still pending or allowed fill lost: %+v, %v", item, err)
	}
	for _, fill := range excluded {
		if added, err := db.Enqueue(fill); err != nil || added {
			t.Fatal("excluded pending fill lost its deduplication marker")
		}
	}
}

func TestDeliveryWaitsForOrderCompletion(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	partial := completedFill()
	partial.Status, partial.Commission, partial.CommissionAsset = "PARTIALLY_FILLED", "0.1", "USDT"
	if _, err := db.Enqueue(partial); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	notify := make(chan struct{}, 1)
	messages := make(chan string, 2)
	result := make(chan error, 1)
	go func() {
		result <- Deliver(ctx, db, sendFunc(func(_ context.Context, message string) error {
			messages <- message
			return nil
		}), config.Config{Location: time.UTC, PartialFills: true}, slog.New(slog.NewTextHandler(io.Discard, nil)), notify, func(bool) {})
	}()
	select {
	case message := <-messages:
		t.Fatalf("partial fill was sent before order completion: %s", message)
	case <-time.After(100 * time.Millisecond):
	}
	final := partial
	final.TradeID, final.Status, final.CumulativeQuantity, final.Commission = 3, "FILLED", "2", "0.2"
	if _, err := db.Enqueue(final); err != nil {
		t.Fatal(err)
	}
	notify <- struct{}{}
	select {
	case message := <-messages:
		for _, want := range []string{"成交量：2", "成交额：20", "手续费：0.3 USDT", "成交笔数：2"} {
			if !strings.Contains(message, want) {
				t.Errorf("missing %q in %s", want, message)
			}
		}
	case <-ctx.Done():
		t.Fatal("completed order was not delivered")
	}
	for {
		item, err := db.Next()
		if err != nil {
			t.Fatal(err)
		}
		if item == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("order was not acknowledged")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestPartialOrderClosedNotifications(t *testing.T) {
	for _, partialFills := range []bool{true, false} {
		t.Run(fmt.Sprint(partialFills), func(t *testing.T) {
			db, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			partial := completedFill()
			partial.Status, partial.Commission, partial.CommissionAsset = "PARTIALLY_FILLED", "0.1", "USDT"
			closed := partial
			closed.TradeID, closed.Status = -1, "CANCELED"
			complete := completedFill()
			complete.OrderID = 2
			for _, fill := range []trade.Fill{partial, closed, complete} {
				if _, err := db.Enqueue(fill); err != nil {
					t.Fatal(err)
				}
			}
			permanent := &telegram.Error{Code: 403, Permanent: true}
			attempts := 0
			err = Deliver(context.Background(), db, sendFunc(func(_ context.Context, message string) error {
				attempts++
				if partialFills {
					if !strings.Contains(message, "部分成交后撤单") || !strings.Contains(message, "手续费：0.1 USDT") || !strings.Contains(message, "成交笔数：1") {
						t.Errorf("unexpected canceled order summary: %s", message)
					}
				} else if !strings.Contains(message, "订单 ID：<code>2</code>") {
					t.Errorf("canceled order should be skipped: %s", message)
				}
				return permanent
			}), config.Config{Location: time.UTC, PartialFills: partialFills}, slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}), func(bool) {})
			if !errors.Is(err, permanent) || attempts != 1 {
				t.Fatalf("unexpected delivery result: %d attempts, %v", attempts, err)
			}
			if !partialFills {
				item, err := db.Next()
				if err != nil || item == nil || item.Fill.OrderID != 2 {
					t.Fatalf("skipped canceled order was not cleared: %+v, %v", item, err)
				}
			}
		})
	}
}
