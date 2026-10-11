package trade

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

var ErrStreamEnded = errors.New("user data stream ended")

type Fill struct {
	Market             string `json:"market"`
	Symbol             string `json:"symbol"`
	Side               string `json:"side"`
	OrderType          string `json:"order_type"`
	Status             string `json:"status"`
	OrderID            int64  `json:"order_id"`
	TradeID            int64  `json:"trade_id"`
	Time               int64  `json:"time"`
	Price              string `json:"price"`
	Quantity           string `json:"quantity"`
	CumulativeQuantity string `json:"cumulative_quantity"`
	AveragePrice       string `json:"average_price"`
	QuoteQuantity      string `json:"quote_quantity"`
	Commission         string `json:"commission"`
	CommissionAsset    string `json:"commission_asset"`
	RealizedProfit     string `json:"realized_profit,omitempty"`
	PositionSide       string `json:"position_side,omitempty"`
	Maker              bool   `json:"maker"`
	ReduceOnly         bool   `json:"reduce_only"`
}

func (f Fill) Key() string {
	return fmt.Sprintf("%s:%s:%d:%d", f.Market, f.Symbol, f.OrderID, f.TradeID)
}

// Binance distinguishes l/L, x/X, s/S, ap/AP and other case variants.
// Go's normal struct decoding falls back to case-insensitive matches, so decode
// only exact map keys in decodeExecution, even for fields we otherwise ignore.
type execution struct {
	Symbol     string `json:"s"`
	Side       string `json:"S"`
	Type       string `json:"o"`
	Execution  string `json:"x"`
	Status     string `json:"X"`
	OrderID    int64  `json:"i"`
	TradeID    *int64 `json:"t"`
	Time       int64  `json:"T"`
	Quantity   string `json:"l"`
	Price      string `json:"L"`
	Cumulative string `json:"z"`
	Quote      string `json:"Z"`
	LastQuote  string `json:"Y"`
	Average    string `json:"ap"`
	Commission string `json:"n"`
	Asset      string `json:"N"`
	Profit     string `json:"rp"`
	Position   string `json:"ps"`
	Maker      bool   `json:"m"`
	ReduceOnly bool   `json:"R"`
}

// Parse ignores account updates, new/canceled orders and TRADE_LITE duplicate events.
func Parse(market string, data []byte, partial bool) (*Fill, error) {
	if market != "spot" && market != "futures" {
		return nil, errors.New("unsupported market")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, errors.New("invalid event JSON")
	}
	if event := fields["event"]; len(event) > 0 {
		data = event
		fields = nil
		if err := json.Unmarshal(data, &fields); err != nil {
			return nil, errors.New("invalid wrapped event")
		}
	}
	var eventType string
	if raw := fields["e"]; raw != nil {
		if err := json.Unmarshal(raw, &eventType); err != nil {
			return nil, errors.New("invalid event type")
		}
	}
	if eventType == "listenKeyExpired" || eventType == "eventStreamTerminated" || eventType == "serverShutdown" {
		return nil, ErrStreamEnded
	}
	if (market == "spot" && eventType != "executionReport") || (market == "futures" && eventType != "ORDER_TRADE_UPDATE") {
		return nil, nil
	}
	if market == "futures" {
		data = fields["o"]
	}
	e, err := decodeExecution(data)
	if err != nil {
		return nil, errors.New("invalid order event")
	}
	if e.Execution != "TRADE" || (!partial && e.Status != "FILLED") {
		return nil, nil
	}
	qty, err := positive(e.Quantity)
	if err != nil {
		return nil, errors.New("invalid fill quantity")
	}
	price, err := positive(e.Price)
	if err != nil {
		return nil, errors.New("invalid fill price")
	}
	cumulative, err := positive(e.Cumulative)
	if err != nil {
		return nil, errors.New("invalid cumulative quantity")
	}
	if e.Symbol == "" || e.OrderID <= 0 || e.TradeID == nil || *e.TradeID < 0 || e.Time <= 0 || (e.Side != "BUY" && e.Side != "SELL") {
		return nil, errors.New("invalid fill identifiers")
	}
	average := e.Average
	if market == "spot" {
		quote, err := positive(e.Quote)
		if err != nil {
			return nil, errors.New("invalid cumulative quote quantity")
		}
		average = quote.DivRound(cumulative, 16).String()
	}
	if _, err := positive(average); err != nil {
		return nil, errors.New("invalid average price")
	}
	quoteQuantity := qty.Mul(price).String()
	if market == "spot" && e.LastQuote != "" {
		q, err := positive(e.LastQuote)
		if err != nil {
			return nil, errors.New("invalid last quote quantity")
		}
		quoteQuantity = q.String()
	}
	return &Fill{
		Market: market, Symbol: e.Symbol, Side: e.Side, OrderType: e.Type, Status: e.Status,
		OrderID: e.OrderID, TradeID: *e.TradeID, Time: e.Time,
		Price: price.String(), Quantity: qty.String(), CumulativeQuantity: cumulative.String(),
		AveragePrice: average, QuoteQuantity: quoteQuantity,
		Commission: e.Commission, CommissionAsset: e.Asset, RealizedProfit: e.Profit,
		PositionSide: e.Position, Maker: e.Maker, ReduceOnly: e.ReduceOnly,
	}, nil
}

func decodeExecution(data []byte) (execution, error) {
	var e execution
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return e, err
	}
	for _, field := range []struct {
		key  string
		dest any
	}{
		{"s", &e.Symbol}, {"S", &e.Side}, {"o", &e.Type}, {"x", &e.Execution}, {"X", &e.Status},
		{"i", &e.OrderID}, {"t", &e.TradeID}, {"T", &e.Time}, {"l", &e.Quantity}, {"L", &e.Price},
		{"z", &e.Cumulative}, {"Z", &e.Quote}, {"Y", &e.LastQuote}, {"ap", &e.Average},
		{"n", &e.Commission}, {"N", &e.Asset}, {"rp", &e.Profit}, {"ps", &e.Position},
		{"m", &e.Maker}, {"R", &e.ReduceOnly},
	} {
		if raw := fields[field.key]; raw != nil {
			if err := json.Unmarshal(raw, field.dest); err != nil {
				return e, err
			}
		}
	}
	return e, nil
}

func positive(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err == nil && !d.IsPositive() {
		err = errors.New("value is not positive")
	}
	return d, err
}

func (f Fill) Message(location *time.Location) string {
	esc := html.EscapeString
	market := "现货"
	if f.Market == "futures" {
		market = "U 本位合约"
	}
	side := "买入 🟢"
	if f.Side == "SELL" {
		side = "卖出 🔴"
	}
	status := "部分成交"
	if f.Status == "FILLED" {
		status = "完全成交"
	}
	liquidity := "Taker"
	if f.Maker {
		liquidity = "Maker"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<b>币安成交 · %s</b>\n<b>%s</b> | %s\n状态：%s · %s\n", market, esc(f.Symbol), side, status, esc(f.OrderType))
	fmt.Fprintf(&b, "本次成交价：%s\n本次成交量：%s\n本次成交额：%s（计价币）\n累计成交量：%s\n累计均价：%s\n", esc(f.Price), esc(f.Quantity), esc(f.QuoteQuantity), esc(f.CumulativeQuantity), esc(f.AveragePrice))
	fee := f.Commission
	if fee == "" {
		fee = "0"
	}
	fmt.Fprintf(&b, "本次手续费：%s %s\n流动性：%s\n", esc(fee), esc(f.CommissionAsset), liquidity)
	if f.Market == "futures" {
		fmt.Fprintf(&b, "持仓方向：%s\n仅减仓：%t\n本次已实现盈亏：%s（结算币，未扣手续费）\n", esc(f.PositionSide), f.ReduceOnly, esc(f.RealizedProfit))
	}
	fmt.Fprintf(&b, "订单 ID：<code>%d</code>\n成交 ID：<code>%d</code>\n时间：%s", f.OrderID, f.TradeID, time.UnixMilli(f.Time).In(location).Format("2006-01-02 15:04:05.000 -07:00"))
	return b.String()
}
