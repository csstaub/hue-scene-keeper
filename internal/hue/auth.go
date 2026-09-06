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
		// A refusal the bridge spelled out. The whitelist is full, or the
		// body was wrong. It arrives in a 200 like every other CLIP
		// application error, so it classifies as one and PairWithRetry stops
		// on it instead of polling out the rest of the window.
		return "", &EnvelopeError{Method: http.MethodPost, Path: "/api", Description: r.Error.Description}
	default:
		return "", errors.New("unexpected pairing response from bridge")
	}
}

// PairWithRetry polls Pair until the link button is pressed or ctx expires.
// notify, if non-nil, is called before each attempt with the attempt number.
//
// Transient failures keep the poll going. The pairing window is the couple of
// minutes in which the user is standing at the bridge pressing its button. A
// 503 from a busy bridge, or a connection reset because they just re-plugged
// it, is the likeliest error there is. Giving up on one cost them the whole
// ceremony over again. Only a refusal that will not change on its own, or ctx
// ending, stops the loop.
func (c *Client) PairWithRetry(ctx context.Context, appName string, interval time.Duration, notify func(attempt int)) (string, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	// The last transient error. Kept so that running out of time after two
	// minutes of a refusing bridge does not report itself as the user failing
	// to press a button. A later ErrLinkButton clears it. The bridge answered
	// after all, so the button really is what the poll is waiting on.
	var lastErr error
	giveUp := func(cause error) error {
		if lastErr != nil {
			return fmt.Errorf("gave up waiting for the link button: %w", lastErr)
		}
		return fmt.Errorf("timed out waiting for the link button: %w", cause)
	}

	for attempt := 1; ; attempt++ {
		if notify != nil {
			notify(attempt)
		}
		key, err := c.Pair(ctx, appName)
		switch {
		case err == nil:
			return key, nil
		case errors.Is(err, ErrLinkButton):
			lastErr = nil
		case Retryable(err):
			lastErr = err
		case ctx.Err() != nil:
			// Time ran out inside an attempt rather than between two.
			return "", giveUp(ctx.Err())
		default:
			return "", err
		}
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return "", giveUp(ctx.Err())
		case <-t.C:
		}
	}
}
