package trade

import (
	"strings"
	"testing"
	"time"
)

func TestOrderSummary(t *testing.T) {
	fills := []Fill{
		{TradeID: 56062541, Quantity: "2291", QuoteQuantity: "1360.6249", CumulativeQuantity: "2291", RealizedProfit: "1.25", Status: "PARTIALLY_FILLED"},
		{TradeID: 56062542, Quantity: "697", QuoteQuantity: "413.9483", CumulativeQuantity: "2988", RealizedProfit: "4.2517", Status: "PARTIALLY_FILLED"},
		{TradeID: 56062543, Quantity: "345", QuoteQuantity: "204.8955", CumulativeQuantity: "3333", RealizedProfit: "2.1045", Status: "FILLED"},
	}
	var order Order
	for i, fill := range fills {
		fill.Market, fill.Symbol, fill.OrderID = "futures", "TIAUSDC", 2455440319
		fill.Side, fill.OrderType, fill.PositionSide = "BUY", "LIMIT", "BOTH"
		fill.Price, fill.AveragePrice, fill.Commission, fill.CommissionAsset = "0.5939", "0.5939", "0", "USDC"
		fill.Maker, fill.ReduceOnly, fill.Time = true, true, 1791780038696+int64(i)
		if err := order.Add(fill); err != nil {
			t.Fatal(err)
		}
	}
	message := order.Message(time.FixedZone("CST", 8*3600))
	for _, want := range []string{"币安订单汇总", "成交均价：0.5939", "成交量：3333", "成交额：1979.4687", "手续费：0 USDC", "已实现盈亏：7.6062", "成交笔数：3", "流动性：Maker", "订单 ID：<code>2455440319</code>", "+08:00"} {
		if !strings.Contains(message, want) {
			t.Fatalf("missing %q in %s", want, message)
		}
	}
	if !order.Complete() || strings.Contains(message, "本次") || strings.Contains(message, "记录不完整") || strings.Contains(message, "成交 ID") {
		t.Fatalf("expected a complete order summary: %s", message)
	}
}

func TestOrderFeesAndLiquidity(t *testing.T) {
	var order Order
	for _, fill := range []Fill{
		{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 1, Quantity: "1", QuoteQuantity: "10", CumulativeQuantity: "1", AveragePrice: "10", Commission: "0.001", CommissionAsset: "BNB", Maker: true, Time: 1},
		{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 2, Quantity: "1", QuoteQuantity: "20", CumulativeQuantity: "2", CumulativeQuote: "30", AveragePrice: "15", Commission: "-0.2", CommissionAsset: "USDT", Time: 2},
		{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 3, Quantity: "1", QuoteQuantity: "30", CumulativeQuantity: "3", CumulativeQuote: "60", AveragePrice: "20", Commission: "0.002", CommissionAsset: "BNB", Status: "FILLED", Time: 3},
	} {
		if err := order.Add(fill); err != nil {
			t.Fatal(err)
		}
	}
	message := order.Message(time.UTC)
	for _, want := range []string{"手续费：0.003 BNB + -0.2 USDT", "流动性：Maker / Taker", "成交均价：20", "成交额：60", "成交笔数：3"} {
		if !strings.Contains(message, want) {
			t.Fatalf("missing %q in %s", want, message)
		}
	}
}

func TestOrderWithMissingExecutions(t *testing.T) {
	var order Order
	f := Fill{Market: "futures", Symbol: "TIAUSDC", OrderID: 2455440319, TradeID: 56062542, Time: 1, Quantity: "697", QuoteQuantity: "413.9483", CumulativeQuantity: "2988", AveragePrice: "0.5939", RealizedProfit: "4.2517"}
	if err := order.Add(f); err != nil {
		t.Fatal(err)
	}
	f.TradeID, f.Time, f.Quantity, f.QuoteQuantity, f.CumulativeQuantity, f.RealizedProfit, f.Status = 56062543, 2, "345", "204.8955", "3333", "2.1045", "FILLED"
	if err := order.Add(f); err != nil {
		t.Fatal(err)
	}
	message := order.Message(time.UTC)
	for _, want := range []string{"成交量：3333", "已接收成交量：1042", "已接收成交额：618.8438", "已接收成交盈亏：6.3562", "已接收成交笔数：2", "成交记录不完整"} {
		if !strings.Contains(message, want) {
			t.Fatalf("missing %q in %s", want, message)
		}
	}
	if order.Complete() {
		t.Fatal("missing earlier fills must not be treated as a complete record")
	}
}

func TestOrderTerminalUpdateDoesNotAddExecution(t *testing.T) {
	var order Order
	f := Fill{Market: "spot", Symbol: "BTCUSDT", OrderID: 1, TradeID: 1, Time: 1, Quantity: "1", QuoteQuantity: "10", CumulativeQuantity: "1", AveragePrice: "10", Commission: "0.1"}
	if err := order.Add(f); err != nil {
		t.Fatal(err)
	}
	f.TradeID, f.Status, f.Time = -1, "CANCELED", 2
	// Terminal payloads may repeat last-trade amounts; none should be accumulated.
	if err := order.Add(f); err != nil {
		t.Fatal(err)
	}
	if order.TradeCount != 1 || order.Commissions[""].String() != "0.1" || order.Quantity.String() != "1" {
		t.Fatalf("terminal update was counted as a trade: %+v", order)
	}
	if !strings.Contains(order.Message(time.UTC), "部分成交后撤单") {
		t.Fatal("missing terminal status")
	}
}
