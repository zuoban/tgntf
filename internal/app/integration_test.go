package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"tgntf/internal/config"
	"tgntf/internal/store"
	"tgntf/internal/trade"
)

// Exercise both Binance protocols, durable deduplication, Telegram retry and shutdown together.
func TestBothMarketsToTelegram(t *testing.T) {
	t.Run("default", func(t *testing.T) { testBothMarketsToTelegram(t, nil) })
	t.Run("excluded symbols", func(t *testing.T) {
		testBothMarketsToTelegram(t, map[string]struct{}{"BTCUSDT": {}, "ETHUSDT": {}})
	})
}

func testBothMarketsToTelegram(t *testing.T, excludedSymbols map[string]struct{}) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	spot := `{"subscriptionId":0,"event":{"e":"executionReport","E":1728973001334,"s":"BTCUSDT","S":"BUY","o":"LIMIT","O":1728973001000,"r":"NONE","x":"TRADE","X":"FILLED","i":10,"t":20,"T":1728973001334,"l":"0.01","L":"60000","z":"0.01","Z":"600","n":"0","N":null}}`
	futures := `{"e":"ORDER_TRADE_UPDATE","E":1728973001334,"o":{"s":"ETHUSDT","S":"SELL","o":"MARKET","x":"TRADE","X":"FILLED","i":11,"t":21,"T":1728973001334,"l":"1","L":"3000","z":"1","ap":"3000","n":"1.2","N":"USDT","rp":"5","ps":"LONG","R":true}}`
	binance := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		case "/fapi/v1/listenKey":
			_, _ = w.Write([]byte(`{"listenKey":"mock-key"}`))
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		payload := futures
		if r.URL.Path == "/ws-api/v3" {
			var request map[string]any
			if err := conn.ReadJSON(&request); err != nil {
				t.Error(err)
				return
			}
			_ = conn.WriteJSON(map[string]any{"id": request["id"], "status": 200, "result": map[string]int{"subscriptionId": 0}})
			_ = conn.WriteMessage(websocket.TextMessage, []byte(strings.Replace(spot, `"x":"TRADE"`, `"x":"NEW"`, 1)))
			payload = spot
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
		if len(excludedSymbols) > 0 {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(strings.Replace(payload, `"X":"FILLED"`, `"X":"PARTIALLY_FILLED"`, 1)))
			payload = strings.Replace(payload, "USDT", "USDC", 1)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
			_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer binance.Close()
	var attempts atomic.Int32
	messages := make(chan string, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":1}}`))
			return
		}
		var body struct {
			Text   string `json:"text"`
			ChatID string `json:"chat_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ChatID != "1" {
			t.Error("unexpected destination")
		}
		messages <- body.Text
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer telegram.Close()
	path := filepath.Join(t.TempDir(), "notifications.db")
	wsBase := "ws" + strings.TrimPrefix(binance.URL, "http")
	cfg := config.Config{APIKey: "key", SecretKey: "secret", TelegramToken: "123:test", ChatID: "1", Spot: true, Futures: true, PartialFills: true, SpotREST: binance.URL, SpotWS: wsBase + "/ws-api/v3", FuturesREST: binance.URL, FuturesWS: wsBase + "/private/ws", TelegramAPI: telegram.URL, StatePath: path, HealthAddr: "127.0.0.1:0", Location: time.UTC}
	cfg.ExcludedSymbols = excludedSymbols
	spotSymbol, futuresSymbol := "BTCUSDT", "ETHUSDT"
	if len(excludedSymbols) > 0 {
		spotSymbol, futuresSymbol = "BTCUSDC", "ETHUSDC"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- Run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	seen := map[string]bool{}
	for range 2 {
		select {
		case message := <-messages:
			if strings.Contains(message, spotSymbol) {
				seen["spot"] = true
			}
			if strings.Contains(message, futuresSymbol) {
				seen["futures"] = true
			}
		case <-ctx.Done():
			t.Fatal("notification pipeline timed out")
		}
	}
	if !seen["spot"] || !seen["futures"] {
		t.Fatal("one market was not notified")
	}
	// Leave enough time for acknowledgements and assert that duplicate events are not delivered.
	select {
	case extra := <-messages:
		t.Fatalf("duplicate delivery: %s", extra)
	case <-time.After(1200 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application did not stop")
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected one retry and two fills, got %d attempts", attempts.Load())
	}
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if item, err := db.Next(); err != nil || item != nil {
		t.Fatal("delivered notifications were not acknowledged")
	}
	if len(excludedSymbols) > 0 {
		for _, fill := range []trade.Fill{
			{Market: "spot", Symbol: "BTCUSDT", OrderID: 10, TradeID: 20},
			{Market: "futures", Symbol: "ETHUSDT", OrderID: 11, TradeID: 21},
		} {
			if added, err := db.Enqueue(fill); err != nil || !added {
				t.Fatalf("excluded fill should never enter the queue: %s, %v", fill.Key(), err)
			}
		}
	}
}
