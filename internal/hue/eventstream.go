package hue

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// healthyConnection is how long a stream must stay up before we stop counting
// its loss as a consecutive failure. Variable so tests can shorten it.
var healthyConnection = 60 * time.Second

// StreamOptions configure the event stream reader.
type StreamOptions struct {
	// ReadTimeout is how long a connection may go silent before we give up on
	// it and reconnect. Defaults to 15 minutes.
	//
	// The bridge does not keepalive: a capture held open for ten minutes got a
	// single ": hi" at connect and then nothing until a real resource change,
	// with multi-minute gaps on a perfectly live connection. Silence is
	// therefore normal, and a short timeout here only buys needless reconnects,
	// each of which costs a full resync and drops whatever arrives during it.
	// A genuinely dead peer is caught underneath us by net.Dialer.KeepAlive
	// (30s, set in New); this timeout is only a backstop for the case TCP
	// cannot see - the connection healthy but the bridge no longer sending.
	//
	// The effective period is longer than the value set here: lastRead is
	// stamped at connect and the watchdog ticks at ReadTimeout/3, so a wedge is
	// noticed somewhere between one and one-and-a-third timeouts after it
	// starts.
	ReadTimeout time.Duration
	// OnConnect is called after each successful (re)connection, before any
	// events are delivered. Use it to resync the resource cache, since
	// anything that changed while disconnected was never seen.
	//
	// Returning an error abandons the connection and retries with backoff.
	// That matters: without it a failed resync would leave the caller running
	// against an empty cache on a connection that never drops, doing nothing
	// at all until the process is restarted.
	OnConnect func(ctx context.Context) error
	// Logger receives connection lifecycle messages.
	Logger *slog.Logger
}

// Stream consumes the bridge's server-sent event stream, reconnecting with
// linear backoff (2s per consecutive failure, capped at 10 minutes) until ctx
// is cancelled. It only returns on ctx cancellation.
//
// handle is called for each decoded SSE frame, on the reader goroutine, so it
// must not block for long.
//
// Note there is deliberately no last-event-id: the caller resyncs from the
// bridge on every connect, which is authoritative, and replayed history would
// otherwise be indistinguishable from live events.
func (c *Client) Stream(ctx context.Context, opts StreamOptions, handle func([]Event)) error {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	readTimeout := opts.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 15 * time.Minute
	}

	failures := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failures > 0 {
			wait := time.Duration(min(2*failures, 600)) * time.Second
			log.Warn("event stream disconnected, retrying",
				"consecutive_failures", failures, "in", wait)
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}

		start := time.Now()
		err := c.streamOnce(ctx, readTimeout, opts.OnConnect, handle, log)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Warn("event stream ended", "err", err, "uptime", time.Since(start).Round(time.Second))
		}

		// Count consecutive failures, not lifetime disconnects. A bridge that
		// drops a healthy connection every hour must not push the daemon
		// towards the 10-minute cap over its first fortnight of uptime.
		if time.Since(start) >= healthyConnection {
			failures = 0
		} else {
			failures++
		}
	}
}

// stampReader records the time of every successful read, so the watchdog sees
// a connection delivering one very large frame as alive rather than silent.
// Stamping only on completed lines would misfire on exactly the whole-home
// update that takes longest to arrive.
type stampReader struct {
	r     io.Reader
	stamp *atomic.Int64
}

func (s stampReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.stamp.Store(time.Now().UnixNano())
	}
	return n, err
}

// streamOnce holds a single connection open until it fails or ctx is done.
func (c *Client) streamOnce(
	ctx context.Context,
	readTimeout time.Duration,
	onConnect func(context.Context) error,
	handle func([]Event),
	log *slog.Logger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/eventstream/clip/v2"), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if c.appKey != "" {
		req.Header.Set("hue-application-key", c.appKey)
	}

	// Deliberately not rate limited: this is one long-lived connection, and
	// spending a token here could delay a recall behind it.
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errors.New("event stream returned " + resp.Status)
	}
	log.Info("event stream connected", "bridge", c.addr)

	// Watchdog: the http.Client has no Timeout (that would kill the stream),
	// so staleness is enforced here by cancelling the request context.
	var lastRead atomic.Int64
	lastRead.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(readTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, lastRead.Load())) > readTimeout {
					log.Warn("event stream went silent, forcing reconnect", "timeout", readTimeout)
					cancel()
					return
				}
			}
		}
	}()

	if onConnect != nil {
		if err := onConnect(ctx); err != nil {
			return err
		}
	}

	// bufio.Reader rather than Scanner: a whole-home update can exceed
	// Scanner's 64KB token limit, which would silently end the stream.
	br := bufio.NewReaderSize(stampReader{r: resp.Body, stamp: &lastRead}, 64<<10)
	var data strings.Builder

	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			consumeLine(strings.TrimRight(line, "\r\n"), &data, handle)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("bridge closed the event stream")
			}
			return err
		}
	}
}

// consumeLine applies one SSE line, dispatching on the blank line that
// terminates a frame.
func consumeLine(line string, data *strings.Builder, handle func([]Event)) {
	switch {
	case line == "":
		if data.Len() > 0 {
			dispatch(data.String(), handle)
			data.Reset()
		}
		return
	case strings.HasPrefix(line, ":"):
		return // comment line, such as the bridge's ": hi" at connect
	}

	field, value, found := strings.Cut(line, ":")
	if !found {
		return
	}
	value = strings.TrimPrefix(value, " ")

	if field == "data" {
		// Per the SSE spec, repeated data fields are joined with newlines.
		if data.Len() > 0 {
			data.WriteByte('\n')
		}
		data.WriteString(value)
	}
}

func dispatch(payload string, handle func([]Event)) {
	var events []Event
	if err := json.Unmarshal([]byte(payload), &events); err != nil {
		slog.Debug("skipping unparsable event frame", "err", err)
		return
	}
	if len(events) > 0 {
		handle(events)
	}
}
