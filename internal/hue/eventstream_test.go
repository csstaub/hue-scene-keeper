package hue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// streamServer serves one SSE connection per request, writing whatever the
// supplied function produces.
func streamServer(t *testing.T, write func(w http.ResponseWriter, f http.Flusher, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("no flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f.Flush()
		write(w, f, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testClient(url string) *Client {
	return New(Options{BaseURL: url, AppKey: "k", Insecure: true, RequestsPerSecond: 10000})
}

func collectEvents(t *testing.T, srv *httptest.Server, want int, timeout time.Duration) [][]Event {
	t.Helper()
	c := testClient(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan []Event, 32)
	go func() { _ = c.Stream(ctx, StreamOptions{Logger: discardLogger()}, func(evs []Event) { got <- evs }) }()

	var out [][]Event
	deadline := time.After(timeout)
	for len(out) < want {
		select {
		case batch := <-got:
			out = append(out, batch)
		case <-deadline:
			t.Fatalf("timed out after %d/%d batches", len(out), want)
		}
	}
	return out
}

func TestStreamParsesSingleFrame(t *testing.T) {
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		_, _ = fmt.Fprint(w, "id: 1\ndata: [{\"id\":\"1\",\"type\":\"update\",\"data\":[{\"id\":\"l1\",\"type\":\"light\"}]}]\n\n")
		f.Flush()
		<-r.Context().Done()
	})

	batches := collectEvents(t, srv, 1, 5*time.Second)
	if len(batches[0]) != 1 || batches[0][0].Type != "update" {
		t.Fatalf("unexpected batch: %+v", batches[0])
	}
	if PeekType(batches[0][0].Data[0]) != TypeLight {
		t.Fatalf("expected a light resource")
	}
}

// TestStreamIgnoresCommentsAndBlankLines: the bridge opens every connection
// with a ": hi" comment line, which must not be mistaken for a frame
// terminator.
func TestStreamIgnoresCommentsAndBlankLines(t *testing.T) {
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		_, _ = fmt.Fprint(w, ": hi\n\n: hi again\n\n")
		f.Flush()
		_, _ = fmt.Fprint(w, "id: 7\ndata: [{\"id\":\"7\",\"type\":\"update\",\"data\":[]}]\n\n")
		f.Flush()
		<-r.Context().Done()
	})

	batches := collectEvents(t, srv, 1, 5*time.Second)
	if len(batches) != 1 || batches[0][0].ID != "7" {
		t.Fatalf("unexpected batches: %+v", batches)
	}
}

// TestStreamJoinsMultipleDataLines covers the SSE rule that repeated data
// fields in one frame are concatenated with newlines.
func TestStreamJoinsMultipleDataLines(t *testing.T) {
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		_, _ = fmt.Fprint(w, "id: 2\ndata: [{\"id\":\"2\",\"type\":\"update\",\n")
		_, _ = fmt.Fprint(w, "data: \"data\":[]}]\n\n")
		f.Flush()
		<-r.Context().Done()
	})

	batches := collectEvents(t, srv, 1, 5*time.Second)
	if batches[0][0].ID != "2" {
		t.Fatalf("multi-line data was not joined: %+v", batches[0])
	}
}

// TestStreamHandlesFrameSplitAcrossWrites: TCP gives no framing guarantees, so
// a frame may arrive in pieces.
func TestStreamHandlesFrameSplitAcrossWrites(t *testing.T) {
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		payload := "id: 3\ndata: [{\"id\":\"3\",\"type\":\"update\",\"data\":[]}]\n\n"
		for _, chunk := range []string{payload[:10], payload[10:25], payload[25:]} {
			_, _ = fmt.Fprint(w, chunk)
			f.Flush()
			time.Sleep(20 * time.Millisecond)
		}
		<-r.Context().Done()
	})

	batches := collectEvents(t, srv, 1, 5*time.Second)
	if batches[0][0].ID != "3" {
		t.Fatalf("split frame not reassembled: %+v", batches[0])
	}
}

// TestStreamHandlesOversizedFrame guards the reason we use bufio.Reader rather
// than bufio.Scanner: a whole-home update easily exceeds Scanner's 64KB limit,
// which would silently truncate the stream.
func TestStreamHandlesOversizedFrame(t *testing.T) {
	bigName := strings.Repeat("x", 300*1024)
	events := []Event{{ID: "9", Type: "update", Data: []json.RawMessage{
		json.RawMessage(fmt.Sprintf(`{"id":"l1","type":"light","metadata":{"name":%q}}`, bigName)),
	}}}
	payload, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}

	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		_, _ = fmt.Fprintf(w, "id: 9\ndata: %s\n\n", payload)
		f.Flush()
		<-r.Context().Done()
	})

	batches := collectEvents(t, srv, 1, 10*time.Second)
	var light Light
	if err := json.Unmarshal(batches[0][0].Data[0], &light); err != nil {
		t.Fatal(err)
	}
	if len(light.Name()) != len(bigName) {
		t.Fatalf("oversized frame truncated: got %d bytes", len(light.Name()))
	}
}

// TestStreamSendsNoLastEventID locks in a deliberate decision: the caller
// resyncs from the bridge on every connect, which is authoritative. Asking the
// bridge to replay history as well would deliver stale events that are
// indistinguishable from live ones - a light that was switched on and off again
// during the outage would be replayed as a fresh switch-on and recall a room
// whose lights are all off.
func TestStreamSendsNoLastEventID(t *testing.T) {
	var mu sync.Mutex
	var seen []string

	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("last-event-id"))
		n := len(seen)
		mu.Unlock()

		if n == 1 {
			_, _ = fmt.Fprint(w, "id: 42\ndata: [{\"id\":\"42\",\"type\":\"update\",\"data\":[]}]\n\n")
			f.Flush()
			return // drop, forcing a reconnect
		}
		<-r.Context().Done()
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _ = c.Stream(ctx, StreamOptions{Logger: discardLogger()}, func([]Event) {}) }()

	waitUntil(t, 12*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 2
	}, "a reconnect")

	mu.Lock()
	defer mu.Unlock()
	for i, got := range seen {
		if got != "" {
			t.Errorf("connection %d sent last-event-id %q; replay must not be requested", i+1, got)
		}
	}
}

// TestBackoffResetsAfterHealthyConnection guards a bug that only shows up after
// days of uptime: counting lifetime disconnects rather than consecutive
// failures drives a long-running daemon to the 10-minute retry cap, so it goes
// deaf for ten minutes after each of the bridge's routine drops.
func TestBackoffResetsAfterHealthyConnection(t *testing.T) {
	prev := healthyConnection
	healthyConnection = 50 * time.Millisecond
	t.Cleanup(func() { healthyConnection = prev })

	var mu sync.Mutex
	var connects []time.Time

	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		mu.Lock()
		connects = append(connects, time.Now())
		mu.Unlock()
		// Stay up comfortably past healthyConnection, then drop.
		time.Sleep(120 * time.Millisecond)
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = c.Stream(ctx, StreamOptions{Logger: discardLogger()}, func([]Event) {}) }()

	waitUntil(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(connects) >= 4
	}, "four connections")

	mu.Lock()
	defer mu.Unlock()
	// Without the reset the third gap would already be 4s and this would fail.
	total := connects[len(connects)-1].Sub(connects[0])
	if total > 3*time.Second {
		t.Fatalf("backoff did not reset: %d healthy connections spanned %s", len(connects), total)
	}
}

// TestOnConnectErrorDropsConnectionAndRetries covers the daemon's worst
// failure mode: a resync that fails once must not leave it attached to a
// healthy connection with an empty cache, silently doing nothing forever.
func TestOnConnectErrorDropsConnectionAndRetries(t *testing.T) {
	prev := healthyConnection
	healthyConnection = 10 * time.Millisecond
	t.Cleanup(func() { healthyConnection = prev })

	var mu sync.Mutex
	connects := 0

	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		mu.Lock()
		connects++
		mu.Unlock()
		<-r.Context().Done()
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var attempts atomic.Int32
	go func() {
		_ = c.Stream(ctx, StreamOptions{
			Logger: discardLogger(),
			OnConnect: func(context.Context) error {
				if attempts.Add(1) == 1 {
					return errors.New("resync boom")
				}
				return nil
			},
		}, func([]Event) {})
	}()

	waitUntil(t, 8*time.Second, func() bool { return attempts.Load() >= 2 }, "a retry after the failed resync")

	mu.Lock()
	defer mu.Unlock()
	if connects < 2 {
		t.Fatalf("a failed OnConnect must drop the connection; connections=%d", connects)
	}
}

// TestStreamWatchdogReconnectsOnSilence: the watchdog is the only thing that
// notices a bridge which holds the connection open but stops sending, since
// TCP sees nothing wrong. Its default period is deliberately long - silence is
// normal on this bridge - so this drives it with an explicit short one.
func TestStreamWatchdogReconnectsOnSilence(t *testing.T) {
	prev := healthyConnection
	healthyConnection = 10 * time.Millisecond
	t.Cleanup(func() { healthyConnection = prev })

	var connects atomic.Int32
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		connects.Add(1)
		<-r.Context().Done() // never send anything
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		_ = c.Stream(ctx, StreamOptions{
			Logger:      discardLogger(),
			ReadTimeout: 90 * time.Millisecond,
		}, func([]Event) {})
	}()

	waitUntil(t, 8*time.Second, func() bool { return connects.Load() >= 2 },
		"the watchdog to tear down a wedged connection")
}

func TestStreamCallsOnConnect(t *testing.T) {
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		<-r.Context().Done()
	})
	c := testClient(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan struct{}, 1)
	go func() {
		_ = c.Stream(ctx, StreamOptions{
			Logger:    discardLogger(),
			OnConnect: func(context.Context) error { connected <- struct{}{}; return nil },
		}, func([]Event) {})
	}()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnect was never called")
	}
}

// waitUntil polls cond until it holds or the timeout expires.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
