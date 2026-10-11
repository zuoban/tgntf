package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	APIKey, SecretKey, TelegramToken, ChatID string
	Spot, Futures, PartialFills              bool
	SpotREST, SpotWS, FuturesREST, FuturesWS string
	TelegramAPI, StatePath, HealthAddr       string
	Location                                 *time.Location
}

// Load accepts a lookup function so configuration can be tested without process globals.
func Load(getenv func(string) string) (Config, error) {
	c := Config{
		APIKey:        strings.TrimSpace(getenv("BINANCE_API_KEY")),
		SecretKey:     strings.TrimSpace(getenv("BINANCE_SECRET_KEY")),
		TelegramToken: strings.TrimSpace(getenv("TELEGRAM_BOT_TOKEN")),
		ChatID:        strings.TrimSpace(getenv("TELEGRAM_CHAT_ID")),
		SpotREST:      value(getenv, "BINANCE_SPOT_REST_URL", "https://api.binance.com"),
		SpotWS:        value(getenv, "BINANCE_SPOT_WS_URL", "wss://ws-api.binance.com:443/ws-api/v3"),
		FuturesREST:   value(getenv, "BINANCE_FUTURES_REST_URL", "https://fapi.binance.com"),
		FuturesWS:     value(getenv, "BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/private/ws"),
		TelegramAPI:   value(getenv, "TELEGRAM_API_URL", "https://api.telegram.org"),
		StatePath:     value(getenv, "STATE_PATH", "data/notifications.db"),
		HealthAddr:    value(getenv, "HEALTH_ADDR", "127.0.0.1:8080"),
	}
	for _, field := range []struct{ name, value string }{
		{"BINANCE_API_KEY", c.APIKey}, {"BINANCE_SECRET_KEY", c.SecretKey},
		{"TELEGRAM_BOT_TOKEN", c.TelegramToken}, {"TELEGRAM_CHAT_ID", c.ChatID},
	} {
		if field.value == "" {
			return c, fmt.Errorf("missing environment variable %s", field.name)
		}
	}
	// Numeric IDs support private chats, groups and supergroups. Never derive a destination from updates.
	if id, err := strconv.ParseInt(c.ChatID, 10, 64); err != nil || id == 0 {
		return c, fmt.Errorf("TELEGRAM_CHAT_ID must be a nonzero integer")
	}
	if strings.ContainsAny(c.TelegramToken, "/?# \t\r\n") || !strings.Contains(c.TelegramToken, ":") {
		return c, fmt.Errorf("TELEGRAM_BOT_TOKEN has an invalid format")
	}
	var err error
	if c.Spot, err = boolean(getenv, "ENABLE_SPOT", true); err != nil {
		return c, err
	}
	if c.Futures, err = boolean(getenv, "ENABLE_FUTURES", true); err != nil {
		return c, err
	}
	if c.PartialFills, err = boolean(getenv, "NOTIFY_PARTIAL_FILLS", true); err != nil {
		return c, err
	}
	if !c.Spot && !c.Futures {
		return c, fmt.Errorf("enable at least one Binance market")
	}
	c.Location, err = time.LoadLocation(value(getenv, "NOTIFY_TIMEZONE", "Asia/Shanghai"))
	if err != nil {
		return c, fmt.Errorf("NOTIFY_TIMEZONE is invalid")
	}
	for _, endpoint := range []struct{ name, value, scheme string }{
		{"BINANCE_SPOT_REST_URL", c.SpotREST, "https"}, {"BINANCE_SPOT_WS_URL", c.SpotWS, "wss"},
		{"BINANCE_FUTURES_REST_URL", c.FuturesREST, "https"}, {"BINANCE_FUTURES_WS_URL", c.FuturesWS, "wss"},
		{"TELEGRAM_API_URL", c.TelegramAPI, "https"},
	} {
		u, parseErr := url.Parse(endpoint.value)
		if parseErr != nil || u.Scheme != endpoint.scheme || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("%s must be a %s URL without credentials, query or fragment", endpoint.name, endpoint.scheme)
		}
	}
	return c, nil
}

func value(getenv func(string) string, name, fallback string) string {
	if v := strings.TrimSpace(getenv(name)); v != "" {
		return v
	}
	return fallback
}

func boolean(getenv func(string) string, name string, fallback bool) (bool, error) {
	if v := strings.TrimSpace(getenv(name)); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s must be true or false", name)
		}
		return b, nil
	}
	return fallback, nil
}
