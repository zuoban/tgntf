package trade

import (
	"encoding/json"
	"errors"
	"fmt"

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
	CumulativeQuote    string `json:"cumulative_quote,omitempty"`
	Commission         string `json:"commission"`
	CommissionAsset    string `json:"commission_asset"`
	RealizedProfit     string `json:"realized_profit,omitempty"`
	PositionSide       string `json:"position_side,omitempty"`
	Maker              bool   `json:"maker"`
	ReduceOnly         bool   `json:"reduce_only"`
}

func (f Fill) Key() string {
	if !f.IsTrade() {
		return fmt.Sprintf("%s:status:%s", f.OrderKey(), f.Status)
	}
	return fmt.Sprintf("%s:%s:%d:%d", f.Market, f.Symbol, f.OrderID, f.TradeID)
}

func (f Fill) OrderKey() string {
	return fmt.Sprintf("%s:%s:%d", f.Market, f.Symbol, f.OrderID)
}

func (f Fill) IsTrade() bool { return f.TradeID >= 0 }

func (f Fill) Terminal() bool {
	switch f.Status {
	case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH":
		return true
	}
	return false
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

// Parse retains every trade and terminal updates for orders with executed quantity.
// Orders without executions, account updates and TRADE_LITE duplicates are ignored.
func Parse(market string, data []byte) (*Fill, error) {
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
	terminal := (Fill{Status: e.Status}).Terminal()
	if e.Execution != "TRADE" && (!terminal || e.Execution == "CALCULATED") {
		return nil, nil
	}
	isTrade := e.Execution == "TRADE"
	cumulative, err := positive(e.Cumulative)
	if err != nil {
		if !isTrade {
			if quantity, parseErr := decimal.NewFromString(e.Cumulative); parseErr == nil && quantity.IsZero() {
				return nil, nil
			}
		}
		return nil, errors.New("invalid cumulative quantity")
	}
	if e.Symbol == "" || e.OrderID <= 0 || e.Time <= 0 || (e.Side != "BUY" && e.Side != "SELL") {
		return nil, errors.New("invalid fill identifiers")
	}
	average := e.Average
	cumulativeQuote := ""
	if market == "spot" {
		quote, err := positive(e.Quote)
		if err != nil {
			return nil, errors.New("invalid cumulative quote quantity")
		}
		average = quote.DivRound(cumulative, 16).String()
		cumulativeQuote = quote.String()
	}
	if _, err := positive(average); err != nil {
		return nil, errors.New("invalid average price")
	}
	fill := &Fill{
		Market: market, Symbol: e.Symbol, Side: e.Side, OrderType: e.Type, Status: e.Status,
		OrderID: e.OrderID, TradeID: -1, Time: e.Time,
		CumulativeQuantity: cumulative.String(), AveragePrice: average, CumulativeQuote: cumulativeQuote,
		PositionSide: e.Position, ReduceOnly: e.ReduceOnly,
	}
	if !isTrade {
		return fill, nil
	}
	if e.TradeID == nil || *e.TradeID < 0 {
		return nil, errors.New("invalid fill identifiers")
	}
	qty, err := positive(e.Quantity)
	if err != nil {
		return nil, errors.New("invalid fill quantity")
	}
	price, err := positive(e.Price)
	if err != nil {
		return nil, errors.New("invalid fill price")
	}
	quoteQuantity := qty.Mul(price).String()
	if market == "spot" && e.LastQuote != "" {
		q, err := positive(e.LastQuote)
		if err != nil {
			return nil, errors.New("invalid last quote quantity")
		}
		quoteQuantity = q.String()
	}
	for _, amount := range []string{e.Commission, e.Profit} {
		if amount != "" {
			if _, err := decimal.NewFromString(amount); err != nil {
				return nil, errors.New("invalid commission or realized profit")
			}
		}
	}
	fill.TradeID, fill.Price, fill.Quantity, fill.QuoteQuantity = *e.TradeID, price.String(), qty.String(), quoteQuantity
	fill.Commission, fill.CommissionAsset, fill.RealizedProfit, fill.Maker = e.Commission, e.Asset, e.Profit, e.Maker
	return fill, nil
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
