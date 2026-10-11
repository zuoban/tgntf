package trade

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const spotEvent = `{"subscriptionId":0,"event":{"e":"executionReport","E":1728973001334,"s":"BTCUSDT","S":"BUY","o":"LIMIT","O":1728973001000,"r":"NONE","x":"TRADE","X":"PARTIALLY_FILLED","i":123,"t":456,"T":1728973001334,"l":"0.00100000","L":"65000.12345678","z":"0.00200000","Z":"129.00024691356","Y":"65.00012345678","n":"0.00000100","N":"BNB","m":true}}`
const futuresEvent = `{"e":"ORDER_TRADE_UPDATE","E":1728973001334,"T":1728973001334,"o":{"s":"ETHUSDT","S":"SELL","o":"MARKET","x":"TRADE","X":"FILLED","i":234,"t":567,"T":1728973001334,"l":"0.2","L":"3500.12","z":"0.5","ap":"3499.8","AP":"9476.8","n":"0.350012","N":"USDT","rp":"10.42","ps":"LONG","m":false,"R":true}}`

func TestSpotFill(t *testing.T) {
	f, err := Parse("spot", []byte(spotEvent), true)
	if err != nil {
		t.Fatal(err)
	}
	if f == nil || f.Price != "65000.12345678" || f.Quantity != "0.001" || f.AveragePrice != "64500.12345678" || f.QuoteQuantity != "65.00012345678" || !f.Maker {
		t.Fatalf("incorrect spot fill: %+v", f)
	}
	if f.Key() != "spot:BTCUSDT:123:456" {
		t.Fatal(f.Key())
	}
	if f, err := Parse("spot", []byte(spotEvent), false); err != nil || f != nil {
		t.Fatal("partial fill must be filtered")
	}
	final := strings.Replace(spotEvent, "PARTIALLY_FILLED", "FILLED", 1)
	if f, err := Parse("spot", []byte(final), false); err != nil || f == nil {
		t.Fatal("final fill must be delivered")
	}
}

func TestFuturesFill(t *testing.T) {
	f, err := Parse("futures", []byte(futuresEvent), true)
	if err != nil {
		t.Fatal(err)
	}
	if f == nil || f.Quantity != "0.2" || f.Price != "3500.12" || f.QuoteQuantity != "700.024" || f.AveragePrice != "3499.8" || f.PositionSide != "LONG" || !f.ReduceOnly || f.RealizedProfit != "10.42" {
		t.Fatalf("incorrect futures fill: %+v", f)
	}
	if f.Key() != "futures:ETHUSDT:234:567" {
		t.Fatal(f.Key())
	}
}

func TestIgnoreAndValidate(t *testing.T) {
	for _, payload := range []string{`{"e":"outboundAccountPosition"}`, `{"e":"TRADE_LITE"}`, strings.Replace(spotEvent, `"x":"TRADE"`, `"x":"NEW"`, 1), strings.Replace(spotEvent, `"x":"TRADE"`, `"x":"CANCELED"`, 1)} {
		if f, err := Parse("spot", []byte(payload), true); err != nil || f != nil {
			t.Fatalf("expected ignored event, got %+v %v", f, err)
		}
	}
	for _, payload := range []string{`{"e":"listenKeyExpired"}`, `{"event":{"e":"eventStreamTerminated"}}`, `{"e":"serverShutdown"}`} {
		if _, err := Parse("spot", []byte(payload), true); !errors.Is(err, ErrStreamEnded) {
			t.Fatal("expected terminated stream")
		}
	}
	for _, payload := range []string{"not JSON", strings.Replace(spotEvent, `"l":"0.00100000"`, `"l":"0"`, 1), strings.Replace(spotEvent, `"t":456,`, "", 1), strings.Replace(spotEvent, `"L":"65000.12345678"`, `"L":"invalid"`, 1)} {
		if _, err := Parse("spot", []byte(payload), true); err == nil {
			t.Fatal("expected malformed trade error")
		}
	}
	nullFee := strings.Replace(spotEvent, `"N":"BNB"`, `"N":null`, 1)
	if _, err := Parse("spot", []byte(nullFee), true); err != nil {
		t.Fatal("null commission asset is valid", err)
	}
}

func TestMessage(t *testing.T) {
	f, err := Parse("futures", []byte(futuresEvent), true)
	if err != nil {
		t.Fatal(err)
	}
	f.Symbol = "<ETH&USDT>"
	location := time.FixedZone("CST", 8*3600)
	message := f.Message(location)
	for _, want := range []string{"&lt;ETH&amp;USDT&gt;", "U 本位合约", "完全成交", "本次已实现盈亏：10.42", "+08:00", "<code>567</code>"} {
		if !strings.Contains(message, want) {
			t.Fatalf("missing %q in %s", want, message)
		}
	}
	if strings.Contains(message, "<ETH&USDT>") {
		t.Fatal("unescaped user content")
	}
}
