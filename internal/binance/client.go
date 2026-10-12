package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"tgntf/internal/config"
	"tgntf/internal/trade"
)

type Client struct {
	Config  config.Config
	HTTP    *http.Client
	Dialer  *websocket.Dialer
	Logger  *slog.Logger
	OnReady func(string, bool)
	// SessionLifetime and KeepaliveInterval are configurable here for deterministic tests.
	SessionLifetime   time.Duration
	KeepaliveInterval time.Duration
}

type Handler func(trade.Fill) error
type handlerError struct{ err error }

func (e *handlerError) Error() string { return "persist received fill failed" }
func (e *handlerError) Unwrap() error { return e.err }

type apiError struct {
	Status, Code int
	RetryAfter   time.Duration
}

func (e *apiError) Error() string {
	return fmt.Sprintf("Binance request rejected (HTTP %d, code %d)", e.Status, e.Code)
}
func (e *apiError) permanent() bool {
	return e.Code == -2014 || e.Code == -2015 || e.Code == -1022 || e.Status == 401
}

func New(cfg config.Config, logger *slog.Logger, ready func(string, bool)) *Client {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second
	return &Client{
		Config: cfg, HTTP: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		Dialer: &dialer, Logger: logger, OnReady: ready,
		SessionLifetime: 23*time.Hour + 50*time.Minute, KeepaliveInterval: 30 * time.Minute,
	}
}

// Run owns a single market connection. Other markets and Telegram delivery run independently.
func (c *Client) Run(ctx context.Context, market string, handler Handler) error {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.session(ctx, market, handler)
		c.setReady(market, false)
		if ctx.Err() != nil {
			return nil
		}
		var fatal *handlerError
		var rejected *apiError
		if errors.As(err, &fatal) || (errors.As(err, &rejected) && rejected.permanent()) {
			return err
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		delay := backoff + time.Duration(rand.Int64N(int64(backoff/2)+1))
		if errors.As(err, &rejected) {
			delay = max(delay, rejected.RetryAfter)
			if rejected.Status == 429 {
				delay = max(delay, time.Minute)
			}
			if rejected.Status == 418 {
				delay = max(delay, 15*time.Minute)
			}
		}
		c.Logger.Warn("Binance stream disconnected; reconnecting", "market", market, "error", err, "retry_in", delay)
		if !wait(ctx, delay) {
			return nil
		}
		backoff = min(backoff*2, time.Minute)
	}
	return nil
}

func (c *Client) session(parent context.Context, market string, handler Handler) error {
	ctx, cancel := context.WithTimeout(parent, c.SessionLifetime)
	defer cancel()
	endpoint := c.Config.SpotWS
	var listenKey string
	var offset time.Duration
	var err error
	switch market {
	case "spot":
		offset, err = c.clockOffset(ctx)
	case "futures":
		listenKey, err = c.listenKey(ctx, http.MethodPost)
		endpoint = strings.TrimRight(c.Config.FuturesWS, "/") + "/" + url.PathEscape(listenKey)
	default:
		return errors.New("unsupported Binance market")
	}
	if err != nil {
		return err
	}
	conn, res, err := c.Dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if res != nil {
			res.Body.Close()
			return &apiError{Status: res.StatusCode, RetryAfter: retryHeader(res.Header.Get("Retry-After"))}
		}
		return errors.New("Binance WebSocket connection failed (network or timeout)")
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	if err := conn.SetReadDeadline(time.Now().Add(90 * time.Second)); err != nil {
		return errors.New("set WebSocket deadline failed")
	}
	conn.SetPingHandler(func(payload string) error {
		if err := conn.SetReadDeadline(time.Now().Add(90 * time.Second)); err != nil {
			return err
		}
		return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
	// Closing the connection unblocks ReadMessage on shutdown or session rotation.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	maintenanceErrors := make(chan error, 1)
	maintenanceDone := make(chan struct{})
	defer func() { cancel(); <-maintenanceDone }()
	go func() {
		defer close(maintenanceDone)
		if err := c.maintain(ctx, conn, market, listenKey); err != nil {
			maintenanceErrors <- err
			conn.Close()
		}
	}()
	if market == "spot" {
		request := c.subscription(time.Now().Add(offset).UnixMilli())
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return errors.New("set WebSocket write deadline failed")
		}
		if err := conn.WriteJSON(request); err != nil {
			return errors.New("send signed subscription failed")
		}
	} else {
		c.setReady(market, true)
		c.Logger.Info("Binance user data stream connected", "market", market)
	}
	// An independent timer enforces subscription acknowledgement even when server pings arrive.
	var subscribeTimer *time.Timer
	if market == "spot" {
		subscribeTimer = time.AfterFunc(15*time.Second, func() { conn.Close() })
		defer subscribeTimer.Stop()
	}
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case maintenanceErr := <-maintenanceErrors:
				return maintenanceErr
			default:
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("Binance WebSocket read failed or subscription timed out")
		}
		if err := conn.SetReadDeadline(time.Now().Add(90 * time.Second)); err != nil {
			return errors.New("set WebSocket deadline failed")
		}
		var response struct {
			ID     string `json:"id"`
			Status int    `json:"status"`
			Result struct {
				SubscriptionID *int64 `json:"subscriptionId"`
			} `json:"result"`
			Error struct {
				Code int `json:"code"`
				Data struct {
					RetryAfter int64 `json:"retryAfter"`
				} `json:"data"`
			} `json:"error"`
		}
		if market == "spot" && json.Unmarshal(data, &response) == nil && response.ID == "subscribe" {
			if response.Status != http.StatusOK {
				return &apiError{Status: response.Status, Code: response.Error.Code, RetryAfter: max(0, time.Until(time.UnixMilli(response.Error.Data.RetryAfter)))}
			}
			if response.Result.SubscriptionID == nil {
				return errors.New("Binance subscription acknowledgement is missing subscriptionId")
			}
			subscribeTimer.Stop()
			c.setReady(market, true)
			c.Logger.Info("Binance user data stream subscribed", "market", market)
			continue
		}
		fill, err := trade.Parse(market, data)
		if errors.Is(err, trade.ErrStreamEnded) {
			return err
		}
		if err != nil {
			c.Logger.Warn("Ignored invalid Binance event", "market", market, "error", err)
			continue
		}
		if fill != nil {
			if err := handler(*fill); err != nil {
				return &handlerError{err: err}
			}
		}
	}
}

func (c *Client) maintain(ctx context.Context, conn *websocket.Conn, market, key string) error {
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	keepalive := time.NewTicker(c.KeepaliveInterval)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return errors.New("Binance WebSocket heartbeat failed")
			}
		case <-keepalive.C:
			if market != "futures" {
				continue
			}
			updated, err := c.listenKey(ctx, http.MethodPut)
			if err != nil {
				return err
			}
			if updated != "" && updated != key {
				return errors.New("Binance listenKey changed; resubscription required")
			}
		}
	}
}

func (c *Client) setReady(market string, ready bool) {
	if c.OnReady != nil {
		c.OnReady(market, ready)
	}
}

func (c *Client) subscription(timestamp int64) any {
	payload := fmt.Sprintf("apiKey=%s&recvWindow=5000&timestamp=%d", c.Config.APIKey, timestamp)
	return struct {
		ID     string         `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}{"subscribe", "userDataStream.subscribe.signature", map[string]any{
		"apiKey": c.Config.APIKey, "recvWindow": 5000, "timestamp": timestamp,
		"signature": Sign(c.Config.SecretKey, payload),
	}}
}

func Sign(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Client) clockOffset(ctx context.Context) (time.Duration, error) {
	started := time.Now()
	data, err := c.request(ctx, http.MethodGet, strings.TrimRight(c.Config.SpotREST, "/")+"/api/v3/time", false)
	if err != nil {
		return 0, err
	}
	var response struct {
		ServerTime int64 `json:"serverTime"`
	}
	if json.Unmarshal(data, &response) != nil || response.ServerTime <= 0 {
		return 0, errors.New("invalid Binance server time")
	}
	return time.UnixMilli(response.ServerTime).Sub(started.Add(time.Since(started) / 2)), nil
}

func (c *Client) listenKey(ctx context.Context, method string) (string, error) {
	data, err := c.request(ctx, method, strings.TrimRight(c.Config.FuturesREST, "/")+"/fapi/v1/listenKey", true)
	if err != nil {
		return "", err
	}
	var response struct {
		ListenKey string `json:"listenKey"`
	}
	if json.Unmarshal(data, &response) != nil {
		return "", errors.New("invalid Binance listenKey response")
	}
	if method == http.MethodPost && response.ListenKey == "" {
		return "", errors.New("Binance returned an empty listenKey")
	}
	return response.ListenKey, nil
}

func (c *Client) request(ctx context.Context, method, endpoint string, authenticated bool) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, errors.New("create Binance REST request failed")
	}
	if authenticated {
		req.Header.Set("X-MBX-APIKEY", c.Config.APIKey)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("Binance REST request failed (network or timeout)")
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, errors.New("read Binance REST response failed")
	}
	var response struct {
		Code int `json:"code"`
	}
	_ = json.Unmarshal(data, &response)
	if res.StatusCode < 200 || res.StatusCode >= 300 || response.Code < 0 {
		return nil, &apiError{Status: res.StatusCode, Code: response.Code, RetryAfter: retryHeader(res.Header.Get("Retry-After"))}
	}
	return data, nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func retryHeader(value string) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil {
		return max(0, time.Duration(seconds)*time.Second)
	}
	if until, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(until))
	}
	return 0
}
