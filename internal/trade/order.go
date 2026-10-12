package trade

import (
	"errors"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Order summarizes the distinct executions received for one market/symbol/order ID.
// The exchange's cumulative quantity also lets us detect missing executions.
type Order struct {
	Fill           Fill
	Quantity       decimal.Decimal
	QuoteQuantity  decimal.Decimal
	Commissions    map[string]decimal.Decimal
	RealizedProfit decimal.Decimal
	TradeCount     int
	MakerCount     int
	FirstTime      int64
}

func (o *Order) Add(f Fill) error {
	if o.Fill.Symbol != "" && o.Fill.OrderKey() != f.OrderKey() {
		return errors.New("cannot combine different orders")
	}
	cumulative, err := positive(f.CumulativeQuantity)
	if err != nil {
		return errors.New("invalid cumulative quantity in stored order")
	}
	previous, _ := decimal.NewFromString(o.Fill.CumulativeQuantity)
	if o.Fill.Symbol == "" || cumulative.GreaterThan(previous) || (cumulative.Equal(previous) && f.Time >= o.Fill.Time) {
		// A replayed partial update must not reopen an already completed order.
		status := o.Fill.Status
		o.Fill = f
		if (Fill{Status: status}).Terminal() && !f.Terminal() {
			o.Fill.Status = status
		}
	}
	if !f.IsTrade() {
		return nil
	}
	qty, err := positive(f.Quantity)
	if err != nil {
		return errors.New("invalid quantity in stored fill")
	}
	quote, err := positive(f.QuoteQuantity)
	if err != nil {
		return errors.New("invalid quote quantity in stored fill")
	}
	fee, err := amount(f.Commission)
	if err != nil {
		return errors.New("invalid commission in stored fill")
	}
	profit, err := amount(f.RealizedProfit)
	if err != nil {
		return errors.New("invalid realized profit in stored fill")
	}
	o.Quantity = o.Quantity.Add(qty)
	o.QuoteQuantity = o.QuoteQuantity.Add(quote)
	o.RealizedProfit = o.RealizedProfit.Add(profit)
	if o.Commissions == nil {
		o.Commissions = make(map[string]decimal.Decimal)
	}
	o.Commissions[f.CommissionAsset] = o.Commissions[f.CommissionAsset].Add(fee)
	o.TradeCount++
	if f.Maker {
		o.MakerCount++
	}
	if o.FirstTime == 0 || f.Time < o.FirstTime {
		o.FirstTime = f.Time
	}
	return nil
}

func amount(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(s)
}

func (o Order) Complete() bool {
	cumulative, err := decimal.NewFromString(o.Fill.CumulativeQuantity)
	return err == nil && o.Quantity.Equal(cumulative)
}

func (o Order) Message(location *time.Location) string {
	f := o.Fill
	esc := html.EscapeString
	market := "现货"
	if f.Market == "futures" {
		market = "U 本位合约"
	}
	side := "买入 🟢"
	if f.Side == "SELL" {
		side = "卖出 🔴"
	}
	status := map[string]string{
		"FILLED": "完全成交", "CANCELED": "部分成交后撤单",
		"EXPIRED": "部分成交后过期", "EXPIRED_IN_MATCH": "部分成交后过期",
	}[f.Status]
	if status == "" {
		status = "部分成交"
	}
	liquidity := "Taker"
	if o.TradeCount == 0 {
		liquidity = "未知"
	} else if o.MakerCount == o.TradeCount {
		liquidity = "Maker"
	} else if o.MakerCount > 0 {
		liquidity = "Maker / Taker"
	}
	complete := o.Complete()
	prefix := ""
	if !complete {
		prefix = "已接收"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<b>币安订单汇总 · %s</b>\n<b>%s</b> | %s\n状态：%s · %s\n", market, esc(f.Symbol), side, status, esc(f.OrderType))
	fmt.Fprintf(&b, "成交均价：%s\n成交量：%s\n", esc(f.AveragePrice), esc(f.CumulativeQuantity))
	if f.CumulativeQuote != "" {
		fmt.Fprintf(&b, "成交额：%s（计价币）\n", esc(f.CumulativeQuote))
	} else {
		fmt.Fprintf(&b, "%s成交额：%s（计价币）\n", prefix, o.QuoteQuantity.String())
	}
	if !complete {
		fmt.Fprintf(&b, "已接收成交量：%s\n", o.Quantity.String())
	}
	assets := make([]string, 0, len(o.Commissions))
	for asset := range o.Commissions {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	fees := make([]string, 0, len(assets))
	for _, asset := range assets {
		fees = append(fees, strings.TrimSpace(o.Commissions[asset].String()+" "+esc(asset)))
	}
	if len(fees) == 0 {
		fees = append(fees, "0")
	}
	fmt.Fprintf(&b, "%s手续费：%s\n流动性：%s\n", prefix, strings.Join(fees, " + "), liquidity)
	if f.Market == "futures" {
		profitLabel := "已实现盈亏"
		if !complete {
			profitLabel = "已接收成交盈亏"
		}
		fmt.Fprintf(&b, "持仓方向：%s\n仅减仓：%t\n%s：%s（结算币，未扣手续费）\n", esc(f.PositionSide), f.ReduceOnly, profitLabel, o.RealizedProfit.String())
	}
	fmt.Fprintf(&b, "%s成交笔数：%d\n订单 ID：<code>%d</code>\n", prefix, o.TradeCount, f.OrderID)
	if o.FirstTime != 0 {
		fmt.Fprintf(&b, "首笔成交时间：%s\n", time.UnixMilli(o.FirstTime).In(location).Format("2006-01-02 15:04:05.000 -07:00"))
	}
	fmt.Fprintf(&b, "订单结束时间：%s", time.UnixMilli(f.Time).In(location).Format("2006-01-02 15:04:05.000 -07:00"))
	if !complete {
		b.WriteString("\n成交记录不完整；手续费、盈亏和成交笔数仅统计已接收成交。")
	}
	return b.String()
}
