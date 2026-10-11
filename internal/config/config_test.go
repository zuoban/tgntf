package config

import "testing"

func validEnv() map[string]string {
	return map[string]string{"BINANCE_API_KEY": "key", "BINANCE_SECRET_KEY": "secret", "TELEGRAM_BOT_TOKEN": "123:token", "TELEGRAM_CHAT_ID": "-1001234567890"}
}

func TestLoad(t *testing.T) {
	env := validEnv()
	cfg, err := Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Spot || !cfg.Futures || !cfg.PartialFills || cfg.Location.String() != "Asia/Shanghai" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.FuturesWS != "wss://fstream.binance.com/private/ws" {
		t.Fatal("incorrect futures private stream URL")
	}
	for _, test := range []struct{ name, key, value string }{
		{"missing secret", "BINANCE_SECRET_KEY", ""},
		{"invalid chat", "TELEGRAM_CHAT_ID", "@someone"},
		{"zero chat", "TELEGRAM_CHAT_ID", "0"},
		{"invalid flag", "ENABLE_FUTURES", "maybe"},
		{"invalid timezone", "NOTIFY_TIMEZONE", "nowhere"},
		{"insecure endpoint", "TELEGRAM_API_URL", "http://example.com"},
		{"credential URL", "BINANCE_SPOT_WS_URL", "wss://user:pass@example.com/ws"},
		{"token path injection", "TELEGRAM_BOT_TOKEN", "123:token/other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := validEnv()
			env[test.key] = test.value
			if _, err := Load(func(k string) string { return env[k] }); err == nil {
				t.Fatal("expected invalid config")
			}
		})
	}
	env["ENABLE_SPOT"] = "false"
	env["ENABLE_FUTURES"] = "false"
	if _, err := Load(func(k string) string { return env[k] }); err == nil {
		t.Fatal("expected disabled markets error")
	}
}
