package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"tgntf/internal/config"
	"tgntf/internal/trade"
)

const spotFill = `{"subscriptionId":0,"event":{"e":"executionReport","s":"BTCUSDT","S":"BUY","o":"LIMIT","x":"TRADE","X":"FILLED","i":10,"t":20,"T":1728973001334,"l":"0.01","L":"60000","z":"0.01","Z":"600","n":"0","N":null}}`
const futuresFill = `{"e":"ORDER_TRADE_UPDATE","o":{"s":"ETHUSDT","S":"SELL","o":"MARKET","x":"TRADE","X":"FILLED","i":11,"t":21,"T":1728973001334,"l":"1","L":"3000","z":"1","ap":"3000","n":"1.2","N":"USDT","rp":"5","ps":"LONG","R":true}}`

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestSign(t *testing.T) {
	if got := Sign("key", "The quick brown fox jumps over the lazy dog"); got != "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8" {
		t.Fatal("incorrect HMAC-SHA256 signature", got)
	}
}

func TestSpotSubscriptionAndHeartbeat(t *testing.T) {
	upgrader := websocket.Upgrader{}
	fills := make(chan trade.Fill, 1)
	pongs := make(chan string, 1)
	serverTime := time.Now().Add(10 * time.Minute).UnixMilli()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, serverTime)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		var request struct {
			ID, Method string
			Params     struct {
				APIKey     string `json:"apiKey"`
				Timestamp  int64  `json:"timestamp"`
				RecvWindow int64  `json:"recvWindow"`
				Signature  string `json:"signature"`
			}
		}
		if err := conn.ReadJSON(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Method != "userDataStream.subscribe.signature" || request.Params.APIKey != "key" || request.Params.RecvWindow != 5000 {
			t.Errorf("invalid subscription: %+v", request)
		}
		if diff := request.Params.Timestamp - serverTime; diff < 0 || diff > 5000 {
			t.Error("server clock was not synchronized")
		}
		payload := fmt.Sprintf("apiKey=key&recvWindow=5000&timestamp=%d", request.Params.Timestamp)
		if request.Params.Signature != Sign("secret", payload) {
			t.Error("signature does not match sent params")
		}
		_ = conn.WriteJSON(map[string]any{"id": request.ID, "status": 200, "result": map[string]int{"subscriptionId": 0}})
		_ = conn.WriteMessage(websocket.TextMessage, []byte(spotFill))
		conn.SetPongHandler(func(payload string) error {
			select {
			case pongs <- payload:
			default:
			}
			return nil
		})
		_ = conn.WriteControl(websocket.PingMessage, []byte("copy-this-payload"), time.Now().Add(time.Second))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	client := New(config.Config{APIKey: "key", SecretKey: "secret", SpotREST: s.URL, SpotWS: "ws" + strings.TrimPrefix(s.URL, "http"), PartialFills: true}, quietLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx, "spot", func(fill trade.Fill) error { fills <- fill; return nil }) }()
	select {
	case fill := <-fills:
		if fill.TradeID != 20 {
			t.Fatal(fill)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fill not received")
	}
	select {
	case pong := <-pongs:
		if pong != "copy-this-payload" {
			t.Fatal("ping payload not echoed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pong not received")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not stop on cancellation")
	}
}

func TestFuturesListenKeyKeepaliveAndExpiry(t *testing.T) {
	upgrader := websocket.Upgrader{}
	keptAlive := make(chan struct{}, 1)
	expire := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/listenKey" {
			if r.Header.Get("X-MBX-APIKEY") != "key" {
				t.Error("API key header missing")
			}
			if r.Method == http.MethodPut {
				select {
				case keptAlive <- struct{}{}:
				default:
				}
			} else if r.Method != http.MethodPost {
				t.Error("unexpected listenKey method")
			}
			_, _ = w.Write([]byte(`{"listenKey":"test-key"}`))
			return
		}
		if r.URL.Path != "/private/ws/test-key" {
			t.Error("incorrect private stream path", r.URL.Path)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(futuresFill))
		<-expire
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"listenKeyExpired"}`))
	}))
	defer s.Close()
	client := New(config.Config{APIKey: "key", FuturesREST: s.URL, FuturesWS: "ws" + strings.TrimPrefix(s.URL, "http") + "/private/ws", PartialFills: true}, quietLogger(), nil)
	client.KeepaliveInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fills := make(chan trade.Fill, 1)
	result := make(chan error, 1)
	go func() {
		result <- client.session(ctx, "futures", func(fill trade.Fill) error { fills <- fill; return nil })
	}()
	select {
	case fill := <-fills:
		if fill.RealizedProfit != "5" {
			t.Fatal(fill)
		}
	case <-ctx.Done():
		close(expire)
		t.Fatal("no futures fill")
	}
	select {
	case <-keptAlive:
	case <-ctx.Done():
		close(expire)
		t.Fatal("listenKey keepalive missing")
	}
	close(expire)
	select {
	case err := <-result:
		if !errors.Is(err, trade.ErrStreamEnded) {
			t.Fatal("expiry was not detected", err)
		}
	case <-ctx.Done():
		t.Fatal("expiry did not end session")
	}
}

func TestAuthenticationFailureIsFatalAndRedacted(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"code":-2015,"msg":"invalid secret-key"}`))
	}))
	defer s.Close()
	client := New(config.Config{APIKey: "secret-key", FuturesREST: s.URL}, quietLogger(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Run(ctx, "futures", func(trade.Fill) error { t.Error("unexpected fill"); return nil })
	var rejected *apiError
	if !errors.As(err, &rejected) || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("unexpected auth error: %v", err)
	}
}

func TestSubscriptionRequiresAcknowledgement(t *testing.T) {
	upgrader := websocket.Upgrader{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		var request map[string]any
		_ = conn.ReadJSON(&request)
		_ = conn.WriteJSON(map[string]any{"id": "subscribe", "status": 200, "result": map[string]any{}})
	}))
	defer s.Close()
	ready := false
	client := New(config.Config{APIKey: "key", SecretKey: "secret", SpotREST: s.URL, SpotWS: "ws" + strings.TrimPrefix(s.URL, "http")}, quietLogger(), func(_ string, value bool) { ready = value })
	err := client.session(context.Background(), "spot", func(trade.Fill) error { return nil })
	if err == nil || ready {
		t.Fatal("missing subscriptionId must not set ready")
	}
}

func TestRateLimitHeader(t *testing.T) {
	if retryHeader("120") != 2*time.Minute || retryHeader("invalid") != 0 || retryHeader("-1") != 0 {
		t.Fatal("invalid retry-after handling")
	}
	// Guard against signature data accidentally being serialized as a secret key.
	client := New(config.Config{APIKey: "key", SecretKey: "never-serialize-this"}, quietLogger(), nil)
	request, _ := json.Marshal(client.subscription(123))
	if strings.Contains(string(request), "never-serialize-this") {
		t.Fatal("secret key sent as a parameter")
	}
}

func TestReconnectAfterStreamTermination(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var connections atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		var request map[string]any
		if err := conn.ReadJSON(&request); err != nil {
			t.Error(err)
			return
		}
		_ = conn.WriteJSON(map[string]any{"id": request["id"], "status": 200, "result": map[string]int{"subscriptionId": 0}})
		n := connections.Add(1)
		payload := spotFill
		if n > 1 {
			payload = strings.Replace(payload, `"t":20`, `"t":21`, 1)
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
		if n == 1 {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":{"e":"eventStreamTerminated"}}`))
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	client := New(config.Config{APIKey: "key", SecretKey: "secret", SpotREST: s.URL, SpotWS: "ws" + strings.TrimPrefix(s.URL, "http"), PartialFills: true}, quietLogger(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fills := make(chan trade.Fill, 2)
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx, "spot", func(fill trade.Fill) error { fills <- fill; return nil }) }()
	for _, tradeID := range []int64{20, 21} {
		select {
		case fill := <-fills:
			if fill.TradeID != tradeID {
				t.Fatal(fill)
			}
		case <-ctx.Done():
			t.Fatal("stream did not reconnect and resubscribe")
		}
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnected stream failed to stop")
	}
	if connections.Load() != 2 {
		t.Fatal("unexpected reconnect count")
	}
}
