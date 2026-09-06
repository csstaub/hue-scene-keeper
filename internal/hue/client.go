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
// It lets callers tell a transient refusal apart from a permanent one. A recall
// that failed with 503 is worth retrying. One that failed with 404 means the
// scene is gone, and retrying would only hammer the bridge.
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
// 429 is the bridge asking us to slow down. The 5xx family covers a bridge that
// is busy, restarting, or behind a flaky link. Everything else needs a human. A
// bad app key, a deleted scene, a malformed body. Repeating those only adds
// load.
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

// EnvelopeError is a refusal the bridge reported inside a 2xx response.
//
// CLIP v2 does not use status codes for most application-level failures. An
// unknown scene id, a group that no longer exists, or a body it will not accept
// all come back as HTTP 200 with a populated errors array. Without a type of
// its own, such a refusal reached Retryable as a plain error and fell through
// to "transient". A scene deleted in the Hue app was then recalled again
// through the whole of recallBackoff. That is exactly what StatusError exists
// to prevent.
type EnvelopeError struct {
	Method      string
	Path        string
	Description string
}

func (e *EnvelopeError) Error() string {
	return fmt.Sprintf("bridge refused %s %s: %s", e.Method, e.Path, e.Description)
}

// Retryable reports whether the refusal is worth sending again later.
//
// Always false. The bridge gives no machine-readable code to sort these by,
// only a human-readable description whose wording is not part of any contract.
// The failures it delivers this way are lookups and validation. The resource is
// gone, the id is wrong, the payload is wrong. None of those resolve by asking
// again. Better to report the description once than bury it under a backoff.
func (e *EnvelopeError) Retryable() bool { return false }

// envelopeError returns the bridge's first envelope error for a request, or
// nil if it reported none.
func envelopeError(method, path string, env Envelope) error {
	if len(env.Errors) == 0 {
		return nil
	}
	return &EnvelopeError{Method: method, Path: path, Description: env.Errors[0].Description}
}

// ErrRequestTimeout is returned when a request exceeds the client's per-request
// timeout. It is distinct from the caller's context expiring. A slow bridge is
// worth another try. A shutting-down caller is not.
var ErrRequestTimeout = errors.New("bridge request timed out")

// Retryable reports whether an error from a Client call is worth retrying.
//
// A transport error (connection refused, reset, timed out) is transient by
// nature. So anything that is not a refusal the bridge spelled out, and not a
// canceled context, counts. The bridge spells refusals out two ways, by status
// code and inside a 2xx envelope, and both get to answer for themselves. The
// caller's own context errors do not. It is shutting down or it gave up, and
// there is nobody left to retry for.
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
	// resolves itself. Either the bridge was replaced and the user must
	// re-pair, or something is impersonating it. Retrying buries that in a
	// stream of retry warnings instead of reporting it.
	if errors.Is(err, ErrPinMismatch) {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Retryable()
	}
	var ee *EnvelopeError
	if errors.As(err, &ee) {
		return ee.Retryable()
	}
	return true
}

// Pin holds the trust-on-first-use pin for a bridge's TLS key.
//
// The bridge serves a self-signed certificate, so ordinary chain verification
// cannot work. The client accepts whatever it presents the first time,
// remembers the SHA-256 of its SubjectPublicKeyInfo, and fails closed on any
// later change.
type Pin struct {
	mu    sync.Mutex
	value string
	// learning serializes the trust-on-first-use path, so exactly one
	// handshake learns however many arrive at once. It exists because mu is
	// deliberately not held across OnLearn. See verify.
	learning sync.Mutex
	// OnLearn, if set, is called once when a pin is first recorded so the
	// caller can persist it. It must genuinely persist, because the pin is
	// not trusted unless it returns nil.
	//
	// It is called with none of the Pin's locks held, from inside the TLS
	// handshake. It may read Value() and may take as long as an fsync'd write
	// needs. What it must not do is drive a fresh handshake against this same
	// Pin. That re-enters verify, the one thing the learning lock cannot let
	// through. Set it before the first connection. It is read once, under mu,
	// and never written from here.
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
	have, onLearn := p.value, p.OnLearn
	p.mu.Unlock()
	if have != "" {
		return matchPin(have, got)
	}

	// Learning. The lock held from here is not mu. OnLearn writes the pin to
	// disk, fsync and all, and this is a VerifyPeerCertificate callback on the
	// TLS handshake goroutine. Hold the lock that Value() takes across it and
	// any callback reading its own Pin deadlocks the handshake. A second lock
	// keeps the "learned exactly once" guarantee without that.
	p.learning.Lock()
	defer p.learning.Unlock()

	// Another handshake may have learned while we waited for the lock, in
	// which case this one is an ordinary check against what it recorded.
	p.mu.Lock()
	have = p.value
	p.mu.Unlock()
	if have != "" {
		return matchPin(have, got)
	}

	// Persist before committing. Keep the pin in memory after a failed save
	// and this process carries on happily while nothing was written. Every
	// restart would then re-enter the trust-on-first-use window, with no
	// warning that pinning had silently stopped working.
	if onLearn != nil {
		if err := onLearn(got); err != nil {
			return fmt.Errorf("refusing to trust bridge certificate: could not persist pin: %w", err)
		}
	}
	p.mu.Lock()
	p.value = got
	p.mu.Unlock()
	return nil
}

// matchPin reports whether the certificate we were shown is the one we pinned.
func matchPin(have, got string) error {
	if have == got {
		return nil
	}
	return fmt.Errorf("%w (pinned %s, got %s); if you replaced or factory-reset the bridge, re-pair with `hue-scene-keeper auth --reset-pin`",
		ErrPinMismatch, have, got)
}

// limiter is a token bucket with no burst allowance. It spaces requests evenly.
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
	// Check before reserving. A caller whose context is already dead must not
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
		// back. Otherwise a caller that gives up mid-wait still pushes
		// everyone queued behind it out by a full interval.
		l.release(reserved)
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// release hands back a reservation whose wait was abandoned.
//
// It only rolls back when l.next is still exactly where this caller left it,
// meaning it was the last to reserve. If someone queued behind it in the
// meantime, their slot was picked assuming this one was taken. Subtracting an
// interval now would pull them forward into a gap they are already waiting out.
// Two requests back to back, which is the one thing the limiter prevents.
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
		// The bridge is self-signed. Pin.verify supplies the real check.
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
		// Session resumption. Without it every connection to the bridge is a
		// full handshake. IdleConnTimeout below means the pool is nearly
		// always cold when a recall goes out, since the gap between one room
		// and the next is minutes. The handshake then lands on the critical
		// path of a user-visible event, on a bridge slow enough for it to
		// matter.
		//
		// Note this means Pin.verify is NOT called on a resumed handshake. Go
		// restores PeerCertificates from the session rather than re-verifying.
		// That is safe. Only a peer holding the master secret of a session
		// already pinned can resume one. An impersonator, or a replaced
		// bridge, can only offer a full handshake, which runs the check and
		// fails closed. Set outside the !o.Insecure branch below because the
		// cache is orthogonal to pinning. The size is nominal: one bridge, and
		// with ServerName filled in by the transport the key is the host being
		// talked to.
		ClientSessionCache: tls.NewLRUClientSessionCache(8),
	}
	if !o.Insecure {
		tlsCfg.VerifyPeerCertificate = pin.verify
	}
	transport := &http.Transport{
		TLSClientConfig: tlsCfg,
		// HTTP/1.1 only, deliberately. The event stream's watchdog recovers a
		// wedged bridge by canceling the request context. Under HTTP/2 that
		// only sends RST_STREAM for one stream and leaves the TCP connection
		// in the pool, so the "reconnect" opens a fresh stream on the same
		// dead connection and the daemon never recovers. Under HTTP/1.1
		// canceling closes the connection, which is what the watchdog means.
		// The bridge is a LAN device holding one stream and a trickle of
		// requests, so multiplexing buys nothing here.
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
		// No Timeout. It would also cap the event stream, which must run
		// indefinitely. Per-request deadlines come from the context instead.
		// do() derives one from c.timeout, and Stream deliberately does not.
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

// hostPort normalizes a bridge address for splicing into a URL.
//
// An IPv6 literal has to be bracketed. net/url reads "https://fe80::1/..." as
// a host of "fe80:" on port 1, which no amount of network will fix, and which
// comes back as an error about a port the user never wrote. Everything else
// comes back unchanged: a hostname, an IPv4 literal, an address that already
// carries a port or brackets. So it is safe to apply at every point a URL is
// built.
func hostPort(addr string) string {
	if host, port, err := net.SplitHostPort(addr); err == nil {
		return net.JoinHostPort(host, port)
	}
	// SplitHostPort refuses a bare IPv6 literal for having too many colons,
	// which is exactly the case that needs the brackets. Anything already
	// bracketed fails net.ParseIP and is left alone.
	if ip := net.ParseIP(addr); ip != nil && ip.To4() == nil {
		return "[" + addr + "]"
	}
	return addr
}

func (c *Client) url(path string) string {
	return "https://" + hostPort(c.addr) + path
}

func (c *Client) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	if err := c.lim.wait(ctx); err != nil {
		return nil, err
	}
	// The deadline starts here, after the limiter has released the request,
	// not at entry. A request queued twenty slots deep waits five seconds for
	// its turn. Start the clock before that and it spends most of its budget
	// in the queue, timing out requests that were never actually sent.
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

// classify tells the client's own per-request deadline apart from the caller's
// context ending. Both come out as context.DeadlineExceeded underneath, and
// only the first is worth retrying, so the caller needs help from here.
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
	path := "/clip/v2/resource/" + rtype
	raw, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode %s: %w", rtype, err)
	}
	if err := envelopeError(http.MethodGet, path, env); err != nil {
		return nil, err
	}
	return env.Data, nil
}

// GetLight fetches a single light by id.
func (c *Client) GetLight(ctx context.Context, id string) (Light, error) {
	path := "/clip/v2/resource/light/" + id
	raw, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return Light{}, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Light{}, err
	}
	if err := envelopeError(http.MethodGet, path, env); err != nil {
		return Light{}, err
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
	path := "/clip/v2/resource/smart_scene/" + id
	raw, err := c.do(ctx, http.MethodPut, path, RecallActivate())
	if err != nil {
		return err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode recall response: %w", err)
	}
	return envelopeError(http.MethodPut, path, env)
}
