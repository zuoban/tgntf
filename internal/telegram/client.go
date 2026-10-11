package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	HTTP                   *http.Client
	BaseURL, Token, ChatID string
}

// Error omits response descriptions and request URLs because they may contain secrets.
type Error struct {
	Code       int
	RetryAfter time.Duration
	Permanent  bool
}

func (e *Error) Error() string { return fmt.Sprintf("Telegram sendMessage failed (code %d)", e.Code) }

func New(token, chatID, baseURL string) *Client {
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Token: token, ChatID: chatID, BaseURL: baseURL}
}

func (c *Client) Send(ctx context.Context, text string) error {
	payload, err := json.Marshal(struct {
		ChatID    string `json:"chat_id"`
		Text      string `json:"text"`
		ParseMode string `json:"parse_mode"`
	}{c.ChatID, text, "HTML"})
	if err != nil {
		return errors.New("encode Telegram message failed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/bot"+c.Token+"/sendMessage", bytes.NewReader(payload))
	if err != nil {
		return errors.New("create Telegram request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("Telegram request failed (network or timeout)")
	}
	defer res.Body.Close()
	var response struct {
		OK         bool `json:"ok"`
		Code       int  `json:"error_code"`
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&response)
	if res.StatusCode >= 200 && res.StatusCode < 300 && decodeErr == nil && response.OK {
		return nil
	}
	code := response.Code
	if code == 0 {
		code = res.StatusCode
	}
	if code >= 200 && code < 300 {
		return errors.New("invalid Telegram response")
	}
	retryAfter := time.Duration(response.Parameters.RetryAfter) * time.Second
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &Error{Code: code, RetryAfter: retryAfter, Permanent: code >= 300 && code < 500 && code != 429 && code != 408}
}
