package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/bot123:test/sendMessage" {
			t.Error("incorrect Telegram endpoint")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["chat_id"] != "-100123" || body["text"] != "<b>成交</b>" || body["parse_mode"] != "HTML" {
			t.Errorf("unexpected body: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer s.Close()
	if err := New("123:test", "-100123", s.URL).Send(context.Background(), "<b>成交</b>"); err != nil {
		t.Fatal(err)
	}
}

func TestFailuresAndRedaction(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		body      string
		permanent bool
		retry     time.Duration
	}{
		{"rate limit", 429, `{"ok":false,"error_code":429,"parameters":{"retry_after":3}}`, false, 3 * time.Second},
		{"server", 502, `upstream unavailable`, false, 0},
		{"forbidden", 403, `{"ok":false,"error_code":403,"description":"secret123:token"}`, true, 0},
		{"bad request", 400, `{"ok":false,"error_code":400}`, true, 0},
		{"redirect", 302, `{}`, true, 0},
		{"JSON error on HTTP 200", 200, `{"ok":false,"error_code":401}`, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer s.Close()
			err := New("123:token", "1", s.URL).Send(context.Background(), "fill")
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Permanent != test.permanent || apiErr.RetryAfter != test.retry {
				t.Fatalf("unexpected error: %+v", err)
			}
			if strings.Contains(err.Error(), "token") {
				t.Fatal("secret exposed in error")
			}
		})
	}
	c := New("123:token", "1", "http://127.0.0.1:1")
	if err := c.Send(context.Background(), "fill"); err == nil || strings.Contains(err.Error(), "123:token") {
		t.Fatal("transport error leaks token")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Send(ctx, "fill"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
