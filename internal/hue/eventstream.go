package hue

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// defaultHealthyConnection is how long a stream must stay up before losing it
// stops counting as a consecutive failure.
const defaultHealthyConnection = 60 * time.Second

// maxFrameBytes caps how much of one SSE frame is buffered. A whole-home update
// runs to a few hundred kilobytes, so this is orders of magnitude above
// anything a bridge sends. Its only job is to stop a peer that streams without
// ever sending a newline, or data: lines without the blank line that terminates
// the frame, from growing the heap until the daemon is OOM-killed.
const maxFrameBytes = 8 << 20

// errFrameTooLarge reports that a frame passed maxFrameBytes. It is retryable.
// The frame is abandoned and the connection recycled, and the resync on the
// next connect puts the caller back in step.
var errFrameTooLarge = errors.New("event stream frame too large")

// errStreamSilent reports that the watchdog tore the connection down because
// the bridge stopped sending. It is retryable. Its only job is to stop the
// uptime check treating an open-but-mute connection as healthy.
var errStreamSilent = errors.New("event stream went silent")

// ErrStreamUnreachable reports that the stream failed MaxConsecutiveFailures
// times in a row. The bridge is not answering at the address in use, most often
// because its DHCP lease moved. The caller should rediscover the address rather
// than keep retrying the old one.
var ErrStreamUnreachable = errors.New("event stream unreachable")

// StreamOptions configure the event stream reader.
type StreamOptions struct {
	// ReadTimeout is how long a connection may go silent before it is given up
	// on and reconnected. Defaults to 15 minutes.
	//
	// The bridge does not keepalive. A capture held open for ten minutes got a
	// single ": hi" at connect and then nothing until a real resource change,
	// with multi-minute gaps on a perfectly live connection. Silence is normal,
	// so a short timeout here only buys needless reconnects, each costing a
	// full resync and dropping whatever arrives during it. A genuinely dead
	// peer is caught underneath by net.Dialer.KeepAlive (30s, set in New). This
	// timeout is only a backstop for what TCP cannot see: the connection
	// healthy but the bridge no longer sending.
	//
	// The effective period is longer than the value set here. lastRead is
	// stamped at connect and the watchdog ticks at ReadTimeout/3, so a wedge is
	// noticed somewhere between one and one-and-a-third timeouts after it
	// starts.
	ReadTimeout time.Duration
	// OnConnect is called after each successful (re)connection, before any
	// events are delivered. Use it to resync the resource cache, since
	// anything that changed while disconnected was never seen.
	//
	// Returning an error abandons the connection and retries with backoff.
	// That matters. Without it, a failed resync leaves the caller running
	// against an empty cache on a connection that never drops, doing nothing
	// at all until the process is restarted.
	OnConnect func(ctx context.Context) error
	// Logger receives connection lifecycle messages.
	Logger *slog.Logger
	// MaxConsecutiveFailures gives up with ErrStreamUnreachable after this
	// many failed attempts in a row. Zero retries forever.
	//
	// Retrying forever is wrong when the address itself is stale. A bridge
	// whose DHCP lease moved is never coming back at the old address. The old
	// behavior was to keep trying it until someone edited the credentials file
	// by hand, and that survived restarts, since the address is read back from
	// there. Giving up lets the caller rediscover.
	MaxConsecutiveFailures int
	// HealthyConnection is how long a connection must stay up before losing
	// it stops counting as a consecutive failure. Defaults to 60 seconds.
	//
	// Per-stream rather than a package variable so tests can shorten it
	// without writing shared state that another test's still-running Stream
	// goroutine is reading.
	HealthyConnection time.Duration
}

// Stream consumes the bridge's server-sent event stream, reconnecting with
// linear backoff (2s per consecutive failure, capped at 10 minutes) until ctx
// is canceled. It only returns on ctx cancellation.
//
// handle is called for each decoded SSE frame, on the reader goroutine, so it
// must not block for long.
//
// Note there is deliberately no last-event-id. The caller resyncs from the
// bridge on every connect, which is authoritative, and replayed history would
// be indistinguishable from live events.
func (c *Client) Stream(ctx context.Context, opts StreamOptions, handle func([]Event)) error {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	readTimeout := opts.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 15 * time.Minute
	}
	healthy := opts.HealthyConnection
	if healthy <= 0 {
		healthy = defaultHealthyConnection
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

		// Some failures no amount of retrying will fix. A revoked application
		// key, or a certificate that no longer matches the pin. Retrying those
		// forever leaves the process alive and healthy-looking while it does
		// nothing at all, so the supervisor never learns anything is wrong.
		// Hand them back and let the caller fail loudly.
		if err != nil && !Retryable(err) {
			return err
		}

		// Count consecutive failures, not lifetime disconnects. A bridge that
		// drops a healthy connection every hour must not push the daemon
		// toward the 10-minute cap over its first two weeks of uptime.
		//
		// A watchdog teardown is the exception. The connection was open the
		// whole time, so it always looks "healthy" by uptime. Reset on it and
		// a bridge that accepts connections and then says nothing is retried
		// at full speed forever, never backing off.
		switch {
		case errors.Is(err, errStreamSilent):
			failures++
		case time.Since(start) >= healthy:
			failures = 0
		default:
			failures++
		}

		if opts.MaxConsecutiveFailures > 0 && failures >= opts.MaxConsecutiveFailures {
			return fmt.Errorf("%w: %d consecutive failures against %s: %w",
				ErrStreamUnreachable, failures, c.addr, err)
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

	// Watchdog. The http.Client has no Timeout, since that would kill the
	// stream, so staleness is enforced here by canceling the request context.
	//
	// It is armed *before* the request is sent, not after Do returns. Nothing
	// else bounds the wait for response headers. No Timeout, and no
	// ResponseHeaderTimeout on the transport. So a bridge that accepts the
	// connection, completes TLS, and then never answers would block in Do
	// forever, with the watchdog meant to catch exactly that not yet running.
	// The daemon would go deaf with no log line and no reconnect.
	var lastRead atomic.Int64
	var silent atomic.Bool
	lastRead.Store(time.Now().UnixNano())
	go func() {
		// NewTicker panics on a non-positive period, which a sub-3ns
		// ReadTimeout would produce. Only tests set one anywhere near that
		// low, but the option is exported, and the panic would be unrecovered
		// on this goroutine and take the daemon down with it.
		ticker := time.NewTicker(max(readTimeout/3, 10*time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, lastRead.Load())) > readTimeout {
					log.Warn("event stream went silent, forcing reconnect", "timeout", readTimeout)
					silent.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	// Once the watchdog has fired, everything downstream fails with the
	// derived context's cancellation. Report that as errStreamSilent so the
	// retry loop can tell it apart from a healthy disconnect and back off.
	fail := func(err error) error {
		if silent.Load() {
			return errStreamSilent
		}
		return err
	}

	// Deliberately not rate limited. This is one long-lived connection, and
	// spending a token here could delay a recall behind it.
	resp, err := c.http.Do(req)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		// A StatusError rather than a bare string so Retryable can tell a busy
		// bridge (503, worth waiting for) from a revoked application key (403,
		// which no amount of retrying will fix).
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return &StatusError{
			StatusCode: resp.StatusCode,
			Method:     http.MethodGet,
			Path:       "/eventstream/clip/v2",
			Body:       strings.TrimSpace(string(body)),
		}
	}
	log.Info("event stream connected", "bridge", c.addr)

	if onConnect != nil {
		// The resync issues a handful of rate-limited GETs and stamps nothing,
		// so without this the watchdog would count its own connect work as
		// silence and cancel the connection out from under it.
		lastRead.Store(time.Now().UnixNano())
		if err := onConnect(ctx); err != nil {
			return fail(err)
		}
		lastRead.Store(time.Now().UnixNano())
	}

	// bufio.Reader rather than Scanner. A whole-home update can exceed
	// Scanner's 64KB token limit, which would silently end the stream.
	br := bufio.NewReaderSize(stampReader{r: resp.Body, stamp: &lastRead}, 64<<10)
	var data strings.Builder

	for {
		line, err := readLine(br, maxFrameBytes)
		if len(line) > 0 {
			consumeLine(strings.TrimRight(line, "\r\n"), &data, handle)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fail(errors.New("bridge closed the event stream"))
			}
			return fail(err)
		}
		// The per-line cap does not bound a frame built from many data:
		// lines that never sees its terminating blank line, so the
		// accumulation is capped too.
		if data.Len() > maxFrameBytes {
			return fail(fmt.Errorf("%w: %d bytes with no frame terminator", errFrameTooLarge, data.Len()))
		}
	}
}

// readLine reads one newline-terminated line, refusing to buffer more than
// limit bytes.
//
// bufio.NewReaderSize sets only the *initial* buffer, and ReadString grows
// without bound. A peer that sends bytes and never a newline could then
// allocate until the process dies. ReadSlice fails with ErrBufferFull instead
// of growing, which is what makes the accounting possible.
func readLine(br *bufio.Reader, limit int) (string, error) {
	var line strings.Builder
	for {
		chunk, err := br.ReadSlice('\n')
		if line.Len()+len(chunk) > limit {
			return "", fmt.Errorf("%w: no newline in %d bytes", errFrameTooLarge, line.Len()+len(chunk))
		}
		line.Write(chunk)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line.String(), err
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
