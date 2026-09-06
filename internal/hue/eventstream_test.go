package hue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

// TestStreamIgnoresCommentsAndBlankLines. The bridge opens every connection
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

// TestStreamHandlesFrameSplitAcrossWrites. TCP gives no framing guarantees, so
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

// TestStreamHandlesOversizedFrame guards the reason for bufio.Reader rather
// than bufio.Scanner. A whole-home update easily exceeds Scanner's 64KB limit,
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

// TestStreamAbandonsAnEndlessFrame is the other half of the bufio.Reader
// decision. Dropping Scanner removed the silent 64KB truncation but put nothing
// in its place. Neither ReadString nor a sized bufio.Reader caps anything, since
// the size is only the starting buffer. A peer that writes bytes and never a
// newline, or data: lines and never the blank line that ends the frame, would
// be buffered until the daemon was OOM-killed. Past the cap the frame is
// abandoned and the connection recycled, which the next connect's resync makes
// good.
func TestStreamAbandonsAnEndlessFrame(t *testing.T) {
	cases := map[string]string{
		"no newline":   strings.Repeat("x", 64<<10),
		"no frame end": "data: " + strings.Repeat("x", 64<<10) + "\n",
	}
	for name, chunk := range cases {
		t.Run(name, func(t *testing.T) {
			var reqs atomic.Int32
			srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
				reqs.Add(1)
				// Twice the cap and then silence, so an unbounded reader
				// stalls here rather than eating the machine.
				for sent := 0; sent < 2*maxFrameBytes && r.Context().Err() == nil; sent += len(chunk) {
					if _, err := fmt.Fprint(w, chunk); err != nil {
						return
					}
					f.Flush()
				}
				<-r.Context().Done()
			})

			c := testClient(srv.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = c.Stream(ctx, StreamOptions{
					Logger:            discardLogger(),
					HealthyConnection: 10 * time.Millisecond,
				}, func([]Event) {})
			}()
			t.Cleanup(func() { cancel(); <-done })

			waitUntil(t, 25*time.Second, func() bool { return reqs.Load() >= 2 },
				"the reader to give up on a frame that never ends")
		})
	}
}

// TestStreamSendsNoLastEventID locks in a deliberate decision. The caller
// resyncs from the bridge on every connect, which is authoritative. Asking the
// bridge to replay history as well would deliver stale events indistinguishable
// from live ones. A light switched on and off again during the outage would be
// replayed as a fresh switch-on, recalling a room whose lights are all off.
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
// days of uptime. Counting lifetime disconnects rather than consecutive
// failures drives a long-running daemon to the 10-minute retry cap, so it goes
// deaf for ten minutes after each of the bridge's routine drops.
func TestBackoffResetsAfterHealthyConnection(t *testing.T) {
	var mu sync.Mutex
	var connects []time.Time

	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		mu.Lock()
		connects = append(connects, time.Now())
		mu.Unlock()
		// Stay up comfortably past HealthyConnection, then drop.
		time.Sleep(120 * time.Millisecond)
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stream(ctx, StreamOptions{
			Logger:            discardLogger(),
			HealthyConnection: 50 * time.Millisecond,
		}, func([]Event) {})
	}()
	t.Cleanup(func() { cancel(); <-done })

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

// TestOnConnectErrorDropsConnectionAndRetries covers the daemon's worst failure
// mode. A resync that fails once must not leave it attached to a healthy
// connection with an empty cache, silently doing nothing forever.
func TestOnConnectErrorDropsConnectionAndRetries(t *testing.T) {
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stream(ctx, StreamOptions{
			Logger:            discardLogger(),
			HealthyConnection: 10 * time.Millisecond,
			OnConnect: func(context.Context) error {
				if attempts.Add(1) == 1 {
					return errors.New("resync boom")
				}
				return nil
			},
		}, func([]Event) {})
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitUntil(t, 8*time.Second, func() bool { return attempts.Load() >= 2 }, "a retry after the failed resync")

	mu.Lock()
	defer mu.Unlock()
	if connects < 2 {
		t.Fatalf("a failed OnConnect must drop the connection; connections=%d", connects)
	}
}

// TestStreamWatchdogReconnectsOnSilence. The watchdog is the only thing that
// notices a bridge holding the connection open but sending nothing, since TCP
// sees nothing wrong. Its default period is deliberately long, because silence
// is normal on this bridge, so this drives it with an explicit short one.
func TestStreamWatchdogReconnectsOnSilence(t *testing.T) {
	var connects atomic.Int32
	srv := streamServer(t, func(w http.ResponseWriter, f http.Flusher, r *http.Request) {
		connects.Add(1)
		<-r.Context().Done() // never send anything
	})

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stream(ctx, StreamOptions{
			Logger:            discardLogger(),
			ReadTimeout:       90 * time.Millisecond,
			HealthyConnection: 10 * time.Millisecond,
		}, func([]Event) {})
	}()
	t.Cleanup(func() { cancel(); <-done })

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

// TestStreamWatchdogArmedBeforeRequest. The watchdog must be running before the
// request is sent, not after Do returns. The client has no Timeout, since that
// would cap the stream, and the transport sets no ResponseHeaderTimeout. So a
// bridge that accepts the connection and then never writes a status line is
// bounded by nothing else. Arming afterward left the daemon deaf forever with
// no log line and no reconnect.
func TestStreamWatchdogArmedBeforeRequest(t *testing.T) {
	// Deliberately not streamServer. This handler never writes headers at all.
	var reqs atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stream(ctx, StreamOptions{
			Logger:            discardLogger(),
			ReadTimeout:       90 * time.Millisecond,
			HealthyConnection: 10 * time.Millisecond,
		}, func([]Event) {})
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitUntil(t, 8*time.Second, func() bool { return reqs.Load() >= 2 },
		"the watchdog to abandon a request that never got response headers")
}

// TestStreamWatchdogOpensANewConnection. Canceling the request context must
// actually drop the TCP connection. Under HTTP/2 it only resets one stream and
// returns the connection to the pool, so the reconnect lands on the same dead
// pipe and the watchdog recovers nothing. Counting accepted connections rather
// than requests is what tells the two apart.
func TestStreamWatchdogOpensANewConnection(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // connected, then silent
	}))
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stream(ctx, StreamOptions{
			Logger:            discardLogger(),
			ReadTimeout:       90 * time.Millisecond,
			HealthyConnection: 10 * time.Millisecond,
		}, func([]Event) {})
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitUntil(t, 15*time.Second, func() bool { return conns.Load() >= 3 },
		"each forced reconnect to open a fresh TCP connection")
}

// TestStreamGivesUpOnPermanentRejection. A revoked application key is not worth
// retrying. Looping on it leaves the process alive and healthy-looking while it
// does nothing, so the service manager never learns anything is wrong.
func TestStreamGivesUpOnPermanentRejection(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"errors":[{"description":"unauthorized user"}]}`, http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Stream(ctx, StreamOptions{Logger: discardLogger()}, func([]Event) {})
	if err == nil {
		t.Fatal("a 403 must end the stream, not be retried forever")
	}
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusForbidden {
		t.Fatalf("want a 403 StatusError, got %v", err)
	}
}

// TestStreamGivesUpAfterMaxConsecutiveFailures. A bridge that is not answering
// at the address in use, typically because its DHCP lease moved, has to come
// back as an error so the caller can rediscover. The alternative is retrying
// the old address until someone edits the credentials file.
func TestStreamGivesUpAfterMaxConsecutiveFailures(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	c := testClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := c.Stream(ctx, StreamOptions{
		Logger:                 discardLogger(),
		MaxConsecutiveFailures: 2,
		// Nothing counts as a healthy connection, so every drop counts.
		HealthyConnection: time.Hour,
	}, func([]Event) {})
	if !errors.Is(err, ErrStreamUnreachable) {
		t.Fatalf("want ErrStreamUnreachable, got %v", err)
	}
}
