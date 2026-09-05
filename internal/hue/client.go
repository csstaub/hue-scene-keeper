package hue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ErrPinMismatch is returned when the bridge presents a certificate that does
// not match the one pinned on first contact.
var ErrPinMismatch = errors.New("bridge certificate does not match pinned key")

// DefaultRequestTimeout bounds a single request/response exchange. It is
// applied per request rather than on the http.Client, which would also cap the
// event stream.
const DefaultRequestTimeout = 10 * time.Second

// StatusError is a non-2xx response from the bridge.
//
// It exists so callers can tell a transient refusal apart from a permanent
// one: a recall that failed with 503 is worth retrying, one that failed with
// 404 means the scene is gone and retrying would only hammer the bridge.
type StatusError struct {
	StatusCode int
	Method     string
	Path       string
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("bridge returned %d %s for %s %s: %s",
		e.StatusCode, http.StatusText(e.StatusCode), e.Method, e.Path, e.Body)
}

// Retryable reports whether the request is worth sending again later.
//
// 429 is the bridge asking us to slow down, and the 5xx family covers a bridge
// that is busy, restarting, or behind a flaky link. Everything else - a bad
// app key, a deleted scene, a malformed body - needs a human, and repeating it
// only adds load.
func (e *StatusError) Retryable() bool {
	switch e.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

// ErrRequestTimeout is returned when a request exceeds the client's
// per-request timeout. It is distinct from the caller's context expiring:
// a slow bridge is worth another try, a shutting-down caller is not.
var ErrRequestTimeout = errors.New("bridge request timed out")

// Retryable reports whether an error from a Client call is worth retrying.
//
// A transport error - connection refused, reset, timed out - is transient by
// nature, so anything that is not a StatusError and not a cancelled context
// counts. The caller's own context errors do not: it is shutting down or gave
// up, and there is nobody left to retry for.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRequestTimeout) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// A pin mismatch is the one thing pinning exists to detect, and it never
	// resolves itself: either the bridge was replaced and the user must
	// re-pair, or something is impersonating it. Retrying buries that in a
	// stream of retry warnings instead of surfacing it.
	if errors.Is(err, ErrPinMismatch) {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Retryable()
	}
	return true
}

// Pin holds the trust-on-first-use pin for a bridge's TLS key.
//
// The bridge serves a self-signed certificate, so ordinary chain verification
// cannot work. We accept whatever it presents the first time, remember the
// SHA-256 of its SubjectPublicKeyInfo, and fail closed on any later change.
type Pin struct {
	mu    sync.Mutex
	value string
	// OnLearn, if set, is called once when a pin is first recorded so the
	// caller can persist it.
	OnLearn func(pin string) error
}

// NewPin returns a Pin seeded with a previously stored value ("" to learn).
func NewPin(value string) *Pin { return &Pin{value: value} }

// Value returns the current pin, or "" if nothing has been learned yet.
func (p *Pin) Value() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.value
}

func (p *Pin) verify(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("bridge presented no certificate")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parse bridge certificate: %w", err)
	}
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	got := base64.StdEncoding.EncodeToString(sum[:])

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.value == "" {
		// Persist before committing. If the save fails and we kept the pin in
		// memory anyway, this process would carry on happily while nothing was
		// written - so every restart would re-enter the trust-on-first-use
		// window with no warning that pinning had silently stopped working.
		if p.OnLearn != nil {
			if err := p.OnLearn(got); err != nil {
				return fmt.Errorf("refusing to trust bridge certificate: could not persist pin: %w", err)
			}
		}
		p.value = got
		return nil
	}
	if p.value != got {
		return fmt.Errorf("%w (pinned %s, got %s); if you replaced or factory-reset the bridge, re-pair with `hue-scene-keeper auth --reset-pin`",
			ErrPinMismatch, p.value, got)
	}
	return nil
}

// limiter is a token bucket without burst: it spaces requests evenly.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newLimiter(perSecond float64) *limiter {
	if perSecond <= 0 {
		perSecond = 4
	}
	return &limiter{interval: time.Duration(float64(time.Second) / perSecond)}
}

func (l *limiter) wait(ctx context.Context) error {
	// Check before reserving: a caller whose context is already dead must not
	// consume a slot and push the queue out for everyone behind it.
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	delay := l.next.Sub(now)
	reserved := l.next.Add(l.interval)
	l.next = reserved
	l.mu.Unlock()

	if delay <= 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		// The request will never be sent, so the slot it was holding has to go
		// back; otherwise a caller that gives up mid-wait still pushes everyone
		// queued behind it out by a full interval.
		l.release(reserved)
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// release hands back a reservation whose wait was abandoned.
//
// It only rolls back when l.next is still exactly where we left it, i.e. we
// were the last caller to reserve. If someone queued behind us in the
// meantime, their slot was picked on the assumption that ours was taken, and
// subtracting an interval now would pull them forward into a gap they are
// already waiting out - two requests back to back, which is the one thing the
// limiter exists to prevent.
func (l *limiter) release(reserved time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.next.Equal(reserved) {
		l.next = reserved.Add(-l.interval)
	}
}

// Client talks to one bridge over the CLIP v2 API.
type Client struct {
	addr    string // host or host:port
	appKey  string
	http    *http.Client
	lim     *limiter
	pin     *Pin
	timeout time.Duration
}

// Options configure a Client.
type Options struct {
	// Address is the bridge host, with optional port.
	Address string
	// AppKey is the hue-application-key. Empty is valid for pairing only.
	AppKey string
	// Pin carries the TLS trust-on-first-use state.
	Pin *Pin
	// RequestsPerSecond caps outbound requests. Defaults to 4.
	RequestsPerSecond float64
	// RequestTimeout bounds one request/response exchange, measured from the
	// moment the rate limiter releases it. Defaults to DefaultRequestTimeout.
	RequestTimeout time.Duration
	// Insecure skips certificate pinning entirely. Test use only.
	Insecure bool
	// BaseURL overrides the derived https://<addr> base. Test use only.
	BaseURL string
}

// New builds a Client. It never contacts the bridge.
func New(o Options) *Client {
	pin := o.Pin
	if pin == nil {
		pin = NewPin("")
	}
	tlsCfg := &tls.Config{
		// The bridge is self-signed; Pin.verify supplies the real check.
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	}
	if !o.Insecure {
		tlsCfg.VerifyPeerCertificate = pin.verify
	}
	transport := &http.Transport{
		TLSClientConfig: tlsCfg,
		// HTTP/1.1 only, deliberately. The event stream's watchdog recovers a
		// wedged bridge by cancelling the request context; under HTTP/2 that
		// only sends RST_STREAM for one stream and leaves the TCP connection
		// in the pool, so the "reconnect" opens a fresh stream on the same
		// dead connection and the daemon never recovers. Under HTTP/1.1
		// cancelling closes the connection, which is what the watchdog means.
		// The bridge is a LAN device we hold one stream and a trickle of
		// requests against, so multiplexing buys nothing here.
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        4,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	addr := o.Address
	if o.BaseURL != "" {
		addr = strings.TrimSuffix(strings.TrimPrefix(o.BaseURL, "https://"), "/")
	}
	timeout := o.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	return &Client{
		addr:   addr,
		appKey: o.AppKey,
		// No Timeout: it would also cap the event stream, which must run
		// indefinitely. Per-request deadlines come from the context - do()
		// derives one from c.timeout, and Stream deliberately does not.
		http:    &http.Client{Transport: transport},
		lim:     newLimiter(o.RequestsPerSecond),
		pin:     pin,
		timeout: timeout,
	}
}

// Address returns the bridge address the client is pointed at.
func (c *Client) Address() string { return c.addr }

// Pin returns the client's TLS pin state.
func (c *Client) Pin() *Pin { return c.pin }

func (c *Client) url(path string) string {
	return "https://" + c.addr + path
}

func (c *Client) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	if err := c.lim.wait(ctx); err != nil {
		return nil, err
	}
	// The deadline starts here, after the limiter has released us, not at
	// entry. A request queued twenty slots deep waits five seconds for its
	// turn; starting the clock before that would spend most of its budget in
	// the queue and time out requests that were never actually sent.
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var rdr io.Reader
	if body != nil {
		enc, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(enc)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, c.url(path), rdr)
	if err != nil {
		return nil, err
	}
	if c.appKey != "" {
		req.Header.Set("hue-application-key", c.appKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.classify(ctx, reqCtx, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, c.classify(ctx, reqCtx, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Method:     method,
			Path:       path,
			Body:       truncate(string(raw), 200),
		}
	}
	return raw, nil
}

// classify distinguishes our own per-request deadline from the caller's
// context ending. Both surface as context.DeadlineExceeded underneath, but
// only the first is worth retrying, so the caller cannot tell them apart
// without help from here.
func (c *Client) classify(ctx, reqCtx context.Context, err error) error {
	if ctx.Err() == nil && reqCtx.Err() != nil {
		return fmt.Errorf("%w after %s: %w", ErrRequestTimeout, c.timeout, err)
	}
	return err
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// GetResource fetches every resource of a type, returning the raw entries.
func (c *Client) GetResource(ctx context.Context, rtype string) ([]json.RawMessage, error) {
	raw, err := c.do(ctx, http.MethodGet, "/clip/v2/resource/"+rtype, nil)
	if err != nil {
		return nil, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode %s: %w", rtype, err)
	}
	if len(env.Errors) > 0 {
		return nil, fmt.Errorf("bridge error reading %s: %s", rtype, env.Errors[0].Description)
	}
	return env.Data, nil
}

// GetLight fetches a single light by id.
func (c *Client) GetLight(ctx context.Context, id string) (Light, error) {
	raw, err := c.do(ctx, http.MethodGet, "/clip/v2/resource/light/"+id, nil)
	if err != nil {
		return Light{}, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Light{}, err
	}
	if len(env.Errors) > 0 {
		return Light{}, fmt.Errorf("bridge error reading light %s: %s", id, env.Errors[0].Description)
	}
	if len(env.Data) == 0 {
		return Light{}, fmt.Errorf("light %s not found", id)
	}
	var l Light
	if err := json.Unmarshal(env.Data[0], &l); err != nil {
		return Light{}, err
	}
	return l, nil
}

// RecallSmartScene activates a smart scene, putting the room into its 24-hour
// cycle. The bridge drives every subsequent transition on its own.
func (c *Client) RecallSmartScene(ctx context.Context, id string) error {
	raw, err := c.do(ctx, http.MethodPut, "/clip/v2/resource/smart_scene/"+id, RecallActivate())
	if err != nil {
		return err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode recall response: %w", err)
	}
	if len(env.Errors) > 0 {
		return fmt.Errorf("bridge refused recall of smart scene %s: %s", id, env.Errors[0].Description)
	}
	return nil
}
