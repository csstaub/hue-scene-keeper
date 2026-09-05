package hue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

// ErrLinkButton reports that the bridge is waiting for its link button.
var ErrLinkButton = errors.New("link button not pressed")

type pairResponse struct {
	Success *struct {
		Username  string `json:"username"`
		ClientKey string `json:"clientkey"`
	} `json:"success"`
	Error *struct {
		Type        int    `json:"type"`
		Description string `json:"description"`
	} `json:"error"`
}

// Pair performs one pairing attempt against the bridge. It returns
// ErrLinkButton if the button has not been pressed yet.
//
// This runs through the pinning client, so a successful pairing also learns
// the bridge's TLS key.
func (c *Client) Pair(ctx context.Context, appName string) (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	body := map[string]any{"devicetype": fmt.Sprintf("%s#%s", appName, host)}

	raw, err := c.do(ctx, http.MethodPost, "/api", body)
	if err != nil {
		return "", err
	}
	var responses []pairResponse
	if err := json.Unmarshal(raw, &responses); err != nil {
		return "", fmt.Errorf("decode pairing response: %w", err)
	}
	if len(responses) == 0 {
		return "", errors.New("empty pairing response from bridge")
	}
	r := responses[0]
	switch {
	case r.Success != nil && r.Success.Username != "":
		return r.Success.Username, nil
	case r.Error != nil && r.Error.Type == 101:
		return "", ErrLinkButton
	case r.Error != nil:
		return "", fmt.Errorf("bridge refused pairing: %s", r.Error.Description)
	default:
		return "", errors.New("unexpected pairing response from bridge")
	}
}

// PairWithRetry polls Pair until the link button is pressed or ctx expires.
// notify, if non-nil, is called before each attempt with the attempt number.
func (c *Client) PairWithRetry(ctx context.Context, appName string, interval time.Duration, notify func(attempt int)) (string, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for attempt := 1; ; attempt++ {
		if notify != nil {
			notify(attempt)
		}
		key, err := c.Pair(ctx, appName)
		if err == nil {
			return key, nil
		}
		if !errors.Is(err, ErrLinkButton) {
			return "", err
		}
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return "", fmt.Errorf("timed out waiting for the link button: %w", ctx.Err())
		case <-t.C:
		}
	}
}
