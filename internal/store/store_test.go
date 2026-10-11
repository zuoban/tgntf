package store

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tgntf/internal/trade"
)

func TestDurabilityAndDeduplication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	f := trade.Fill{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 2}
	if added, err := db.Enqueue(f); err != nil || !added {
		t.Fatalf("enqueue: %v %v", added, err)
	}
	if added, err := db.Enqueue(f); err != nil || added {
		t.Fatalf("duplicate: %v %v", added, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	item, err := db.Next()
	if err != nil || item == nil || item.Fill.Key() != f.Key() {
		t.Fatalf("pending not restored: %+v %v", item, err)
	}
	if err := db.Ack(*item, time.Now()); err != nil {
		t.Fatal(err)
	}
	if item, err := db.Next(); err != nil || item != nil {
		t.Fatal("acknowledged fill still pending")
	}
	if added, err := db.Enqueue(f); err != nil || added {
		t.Fatal("delivered duplicate re-enqueued")
	}
	f.TradeID++
	if added, err := db.Enqueue(f); err != nil || !added {
		t.Fatal("distinct fill must not be deduplicated by order")
	}
	f.Market = "futures"
	if added, err := db.Enqueue(f); err != nil || !added {
		t.Fatal("markets must be distinct")
	}
	item, _ = db.Next()
	if item.Fill.Market != "spot" {
		t.Fatal("outbox not in arrival order")
	}
}

func TestConcurrentEnqueueAndPrune(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fill := trade.Fill{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 2}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := db.Enqueue(fill); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	item, _ := db.Next()
	if item == nil {
		t.Fatal("missing concurrent fill")
	}
	if err := db.Prune(time.Now().Add(40 * 24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if added, _ := db.Enqueue(fill); added {
		t.Fatal("prune deleted pending marker")
	}
	if err := db.Ack(*item, time.Now().Add(-40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := db.Prune(time.Now()); err != nil {
		t.Fatal(err)
	}
	if added, _ := db.Enqueue(fill); !added {
		t.Fatal("old delivered marker was not pruned")
	}
}
