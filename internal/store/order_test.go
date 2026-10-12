package store

import (
	"path/filepath"
	"testing"
	"time"

	"tgntf/internal/trade"
)

func orderFill(orderID, tradeID int64, status, cumulative string) trade.Fill {
	return trade.Fill{Market: "futures", Symbol: "ETHUSDT", Side: "BUY", OrderID: orderID, TradeID: tradeID, Status: status,
		Quantity: "1", Price: "10", QuoteQuantity: "10", CumulativeQuantity: cumulative, AveragePrice: "10", Commission: "0.1", CommissionAsset: "USDT", RealizedProfit: "0.2", Time: tradeID}
}

func TestOrderAggregationSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first := orderFill(1, 1, "PARTIALLY_FILLED", "1")
	if _, err := db.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if item, err := db.NextOrder(); err != nil || item != nil {
		t.Fatalf("open order should wait: %+v, %v", item, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	final := orderFill(1, 2, "FILLED", "2")
	for _, fill := range []trade.Fill{first, final, final} {
		if _, err := db.Enqueue(fill); err != nil {
			t.Fatal(err)
		}
	}
	item, err := db.NextOrder()
	if err != nil || item == nil {
		t.Fatalf("completed order missing: %+v, %v", item, err)
	}
	if item.Order.TradeCount != 2 || item.Order.Quantity.String() != "2" || item.Order.QuoteQuantity.String() != "20" || item.Order.Commissions["USDT"].String() != "0.2" || item.Order.RealizedProfit.String() != "0.4" || !item.Order.Complete() {
		t.Fatalf("unexpected totals: %+v", item.Order)
	}
	if err := db.AckOrder(*item, time.Now()); err != nil {
		t.Fatal(err)
	}
	if pending, err := db.Next(); err != nil || pending != nil {
		t.Fatalf("order executions were not acknowledged together: %+v, %v", pending, err)
	}
	for _, fill := range []trade.Fill{first, final, orderFill(1, 3, "FILLED", "2")} {
		if added, err := db.Enqueue(fill); err != nil || added {
			t.Fatalf("completed order must not produce another notification: %v, %v", added, err)
		}
	}
}

func TestOpenOrdersDoNotBlockOtherOrders(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	open := orderFill(1, 1, "PARTIALLY_FILLED", "1")
	spot := orderFill(1, 2, "FILLED", "1")
	spot.Market = "spot"
	otherSymbol := orderFill(1, 3, "FILLED", "1")
	otherSymbol.Symbol = "BTCUSDT"
	otherOrder := orderFill(2, 4, "FILLED", "1")
	for _, fill := range []trade.Fill{open, spot, otherSymbol, otherOrder} {
		if _, err := db.Enqueue(fill); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []trade.Fill{spot, otherSymbol, otherOrder} {
		item, err := db.NextOrder()
		if err != nil || item == nil || item.Order.Fill.OrderKey() != expected.OrderKey() || item.Order.TradeCount != 1 {
			t.Fatalf("orders must stay distinct: %+v, %v", item, err)
		}
		if err := db.AckOrder(*item, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if item, err := db.NextOrder(); err != nil || item != nil {
		t.Fatalf("only open order should remain: %+v, %v", item, err)
	}
	if err := db.Prune(time.Now().Add(40 * 24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if added, err := db.Enqueue(open); err != nil || added {
		t.Fatal("pruning must retain open order deduplication")
	}
	if added, err := db.Enqueue(spot); err != nil || !added {
		t.Fatal("completed order marker must expire with execution markers")
	}
}

func TestConcurrentOrderChangePreservesQueue(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Enqueue(orderFill(1, 1, "FILLED", "1")); err != nil {
		t.Fatal(err)
	}
	item, err := db.NextOrder()
	if err != nil || item == nil {
		t.Fatalf("missing order: %+v, %v", item, err)
	}
	if _, err := db.Enqueue(orderFill(1, 2, "FILLED", "2")); err != nil {
		t.Fatal(err)
	}
	if err := db.AckOrder(*item, time.Now()); err == nil {
		t.Fatal("must not acknowledge an execution absent from the sent summary")
	}
	updated, err := db.NextOrder()
	if err != nil || updated == nil || updated.Order.TradeCount != 2 {
		t.Fatalf("order was lost after concurrent update: %+v, %v", updated, err)
	}
}
