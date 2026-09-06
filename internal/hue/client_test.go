package hue

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestLimiterReturnsCanceledReservation guards the case where a caller gives
// up while queued. Its slot has to go back. Otherwise a context that dies
// mid-wait silently delays every request behind it by a full interval, and
// under a whole-house restore those cancellations arrive in bursts.
func TestLimiterReturnsCanceledReservation(t *testing.T) {
	// Burst 1, so the second wait genuinely queues; with tokens in the bucket
	// it would return at once and never be canceled mid-wait.
	l := newLimiter(10, 1) // 100ms apart

	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := l.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait returned %v, want context.Canceled", err)
	}

	// The abandoned caller reserved the slot 100ms out, so the next caller
	// should inherit it rather than queue behind it at 200ms.
	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("third wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 130*time.Millisecond {
		t.Fatalf("waited %s after a canceled wait; the slot was not returned", elapsed)
	}
}

// TestLimiterKeepsLaterReservationOnRelease covers the concurrent case. By the
// time a canceled caller returns its slot, someone else may have reserved after
// it. Rolling back then would pull that caller forward into a gap it is still
// waiting out, putting two requests on the wire back to back.
func TestLimiterKeepsLaterReservationOnRelease(t *testing.T) {
	l := newLimiter(10, 1)
	mine := time.Now().Add(l.interval)
	theirs := mine.Add(l.interval)
	l.next = theirs

	l.release(mine)

	if !l.next.Equal(theirs) {
		t.Fatalf("release moved next from %v to %v; a later reservation must stand", theirs, l.next)
	}
}

// TestLimiterRejectsDeadContextWithoutReserving. A caller arriving with an
// already-canceled context must not spend a slot on a request it will never
// send.
func TestLimiterRejectsDeadContextWithoutReserving(t *testing.T) {
	// Burst 1 keeps the assertion sharp: with tokens to spare, the follow-up
	// wait would be fast even if the dead context had wrongly spent a slot.
	l := newLimiter(10, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := l.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait on a dead context returned %v, want context.Canceled", err)
	}

	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("waited %s behind a request that was never sent", elapsed)
	}
}

// TestLimiterAllowsBurstThenSpaces is the burst contract: requests that find
// idle capacity leave at once, up to the burst, and the one after them waits a
// full interval. Without the second half this would pass with no limiter at
// all.
func TestLimiterAllowsBurstThenSpaces(t *testing.T) {
	l := newLimiter(10, 3) // 100ms apart, 3 tokens

	start := time.Now()
	for i := range 3 {
		if err := l.wait(context.Background()); err != nil {
			t.Fatalf("burst wait %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("burst of 3 took %s; it should not have queued at all", elapsed)
	}

	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("post-burst wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 70*time.Millisecond {
		t.Fatalf("request after the burst left at %s; the bucket handed out more than it holds", elapsed)
	}
}

// TestLimiterBurstDoesNotRaiseSustainedRate. The burst is start-up capital,
// not income: once it is spent, requests space out at exactly the configured
// interval, so a backlog of N costs (N - burst) intervals no matter how it
// arrives.
func TestLimiterBurstDoesNotRaiseSustainedRate(t *testing.T) {
	l := newLimiter(20, 2) // 50ms apart, 2 tokens

	start := time.Now()
	for i := range 5 {
		if err := l.wait(context.Background()); err != nil {
			t.Fatalf("wait %d: %v", i, err)
		}
	}
	// 2 immediate + 3 spaced = 150ms minimum.
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("5 requests through a 2-token bucket took %s; the rate cap did not hold", elapsed)
	}
}

// TestLimiterBurstCoversLookupRecallPair is the trace that motivated the
// burst: a power-restore lookup GET, a coalesce window of quiet shorter than
// the interval, then the recall PUT. With one token still in the bucket the
// PUT must not inherit the tail of the GET's spacing, which is exactly what
// it did when the bucket held one.
func TestLimiterBurstCoversLookupRecallPair(t *testing.T) {
	l := newLimiter(10, 2) // 100ms apart, 2 tokens

	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("second request waited %s with a token still in the bucket", elapsed)
	}
}

// TestLimiterRefillsWhileIdle: a drained bucket earns its capacity back from
// idle time, one interval per token, so a later isolated request is not still
// paying for a burst that ended long ago.
func TestLimiterRefillsWhileIdle(t *testing.T) {
	l := newLimiter(10, 2) // 100ms apart, 2 tokens

	for i := range 2 {
		if err := l.wait(context.Background()); err != nil {
			t.Fatalf("drain wait %d: %v", i, err)
		}
	}
	// The drain pushed next two intervals out; sleeping past that point means
	// at least one token has come back.
	time.Sleep(250 * time.Millisecond)

	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("wait after idle: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("waited %s after idle time had refilled the bucket", elapsed)
	}
}

func TestStatusErrorRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		status int
		retry  bool
	}{
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusGatewayTimeout, true},
		{http.StatusNotFound, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusBadRequest, false},
	} {
		err := error(&StatusError{StatusCode: tc.status, Method: "PUT", Path: "/x"})
		if got := Retryable(err); got != tc.retry {
			t.Errorf("Retryable(%d) = %v, want %v", tc.status, got, tc.retry)
		}
	}
}

// TestRetryableIgnoresTheCallersContext. A shutting-down daemon has nobody left
// to retry for. The client's own per-request deadline expiring means the bridge
// was slow, and that is worth another try.
func TestRetryableIgnoresTheCallersContext(t *testing.T) {
	if Retryable(context.Canceled) {
		t.Error("a canceled caller must not be retried")
	}
	if Retryable(context.DeadlineExceeded) {
		t.Error("a caller's expired deadline must not be retried")
	}
	if !Retryable(fmt.Errorf("%w: %w", ErrRequestTimeout, context.DeadlineExceeded)) {
		t.Error("our own request timeout must be retried")
	}
	if Retryable(nil) {
		t.Error("nil is not retryable")
	}
	if !Retryable(errors.New("connection refused")) {
		t.Error("a transport error must be retried")
	}
}

// TestRequestTimeoutFiresAndIsDistinguishable. hue.Client has no
// http.Client.Timeout by design, since it would cap the event stream too. This
// is the per-request deadline that stands in for it.
func TestRequestTimeoutFiresAndIsDistinguishable(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)

	c := New(Options{BaseURL: srv.URL, AppKey: "k", Insecure: true, RequestTimeout: 100 * time.Millisecond})
	start := time.Now()
	err := c.RecallSmartScene(context.Background(), "scene-1")
	if err == nil {
		t.Fatal("expected the request to time out")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the timeout did not fire; took %s", took)
	}
	if !errors.Is(err, ErrRequestTimeout) {
		t.Errorf("timeout should be identifiable as ErrRequestTimeout: %v", err)
	}
	if !Retryable(err) {
		t.Errorf("a timed-out request should be retryable: %v", err)
	}
}

// TestRequestTimeoutStartsAfterTheRateLimiter. The deadline covers the request,
// not the wait for a turn. Starting it at entry would time out requests that
// were never sent, and the deeper the queue the more of them.
func TestRequestTimeoutStartsAfterTheRateLimiter(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"rid":"scene-1","rtype":"smart_scene"}]}`))
	}))
	t.Cleanup(srv.Close)

	// One request per second, so the second call waits a second for its turn.
	// That is far longer than the timeout it must not spend while queued.
	c := New(Options{
		BaseURL: srv.URL, AppKey: "k", Insecure: true,
		RequestsPerSecond: 1, RequestTimeout: 300 * time.Millisecond,
	})
	ctx := context.Background()
	if err := c.RecallSmartScene(ctx, "scene-1"); err != nil {
		t.Fatalf("first recall: %v", err)
	}
	if err := c.RecallSmartScene(ctx, "scene-1"); err != nil {
		t.Fatalf("second recall timed out while queued: %v", err)
	}
}

// TestEnvelopeRefusalIsNotRetried. CLIP v2 answers a deleted scene with 200 and
// a populated errors array, not with 404. Read as a transport blip, that
// permanent refusal was recalled again through the whole of recallBackoff.
func TestEnvelopeRefusalIsNotRetried(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"description":"smart scene not found"}],"data":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := New(Options{BaseURL: srv.URL, AppKey: "k", Insecure: true})
	err := c.RecallSmartScene(context.Background(), "scene-1")
	if err == nil {
		t.Fatal("a populated errors array must fail the recall")
	}
	if Retryable(err) {
		t.Errorf("a refusal the bridge spelled out must not be retried: %v", err)
	}
	var ee *EnvelopeError
	if !errors.As(err, &ee) {
		t.Fatalf("want an *EnvelopeError, got %T: %v", err, err)
	}
	if ee.Description != "smart scene not found" {
		t.Errorf("the bridge's description was lost: %q", ee.Description)
	}
	if !strings.Contains(err.Error(), "scene-1") {
		t.Errorf("the error should still name the resource: %v", err)
	}
}

// TestEnvelopeRefusalOnReadsIsNotRetried covers the same envelope on the read
// paths, where a resync would otherwise keep asking for something the bridge
// has already said it will not serve.
func TestEnvelopeRefusalOnReadsIsNotRetried(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"description":"unauthorized user"}],"data":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := New(Options{BaseURL: srv.URL, AppKey: "k", Insecure: true})
	if _, err := c.GetResource(context.Background(), TypeLight); err == nil || Retryable(err) {
		t.Errorf("GetResource: want a non-retryable error, got %v", err)
	}
	if _, err := c.GetLight(context.Background(), "light-1"); err == nil || Retryable(err) {
		t.Errorf("GetLight: want a non-retryable error, got %v", err)
	}
}

// TestClientResumesTLSSessions. Without a ClientSessionCache every connection
// to the bridge is a full handshake, and the pool is nearly always cold when a
// recall goes out. That handshake then sits on the critical path of a
// user-visible event, on a bridge slow enough for it to matter.
func TestClientResumesTLSSessions(t *testing.T) {
	var resumed []bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumed = append(resumed, r.TLS != nil && r.TLS.DidResume)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	t.Cleanup(srv.Close)

	// Pinning left on. This is how production runs, and the point is that the
	// two compose. The first handshake is full, so the pin is still learned.
	c := New(Options{BaseURL: srv.URL, AppKey: "k", RequestsPerSecond: 100})
	transport, _ := c.http.Transport.(*http.Transport)
	for i := range 2 {
		if _, err := c.GetResource(context.Background(), "light"); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		// A fresh connection each time. This is the >90s idle case, where a
		// pooled connection would have closed long before the next recall.
		transport.CloseIdleConnections()
	}

	if len(resumed) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(resumed))
	}
	if resumed[0] {
		t.Error("the first handshake of a process must be full, or nothing is pinned")
	}
	if !resumed[1] {
		t.Error("the second connection paid a full handshake; the session cache is not wired up")
	}
}

// TestPinStillFailsClosedWhenSessionsResume. Go does not call
// VerifyPeerCertificate on a resumed handshake. It restores the peer
// certificates from the session instead. So the session cache moves the pin
// check from per-connection to per-full-handshake. This test says that is still
// safe. Only a peer holding the master secret of an already-pinned session can
// resume one, so a replaced bridge gets a full handshake and the check that
// comes with it.
func TestPinStillFailsClosedWhenSessionsResume(t *testing.T) {
	certA, pinA := selfSignedCert(t)
	certB, _ := selfSignedCert(t)

	var resumed []bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumed = append(resumed, r.TLS != nil && r.TLS.DidResume)
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	// A fixed address, so the replacement bridge answers where the first one
	// did. That is what makes the cached session eligible at all.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	serve := func(ln net.Listener, cert tls.Certificate) *http.Server {
		srv := &http.Server{
			Handler:           handler,
			TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
			ReadHeaderTimeout: 5 * time.Second,
			ErrorLog:          log.New(io.Discard, "", 0),
		}
		go func() { _ = srv.ServeTLS(ln, "", "") }()
		return srv
	}
	first := serve(ln, certA)

	c := New(Options{BaseURL: "https://" + addr, AppKey: "k", RequestsPerSecond: 100})
	transport, _ := c.http.Transport.(*http.Transport)
	for i := range 2 {
		if _, err := c.GetResource(context.Background(), "light"); err != nil {
			t.Fatalf("request %d against the pinned bridge: %v", i, err)
		}
		transport.CloseIdleConnections()
	}
	if got := c.Pin().Value(); got != pinA {
		t.Fatalf("pin = %q, want %q", got, pinA)
	}
	if len(resumed) != 2 || !resumed[1] {
		t.Fatalf("the second connection did not resume (%v); the case under test never arose", resumed)
	}

	// Same address, different key. A replaced bridge, or something pretending
	// to be one.
	_ = first.Close()
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("relisten on %s: %v", addr, err)
	}
	second := serve(ln, certB)
	t.Cleanup(func() { _ = second.Close() })
	transport.CloseIdleConnections()

	_, err = c.GetResource(context.Background(), "light")
	if !errors.Is(err, ErrPinMismatch) {
		t.Fatalf("a different key must not be trusted, got %v", err)
	}
	if got := c.Pin().Value(); got != pinA {
		t.Fatal("a rejected certificate must not replace the stored pin")
	}
}

// TestURLBracketsIPv6Literals. net/url reads "https://fe80::1/..." as the host
// "fe80:" on port 1, so an IPv6 bridge.address never reached the bridge. The
// cloud path's port branch already used net.JoinHostPort, which is how the two
// came to disagree.
func TestURLBracketsIPv6Literals(t *testing.T) {
	for _, tc := range []struct{ addr, wantURL, wantHost string }{
		{"192.168.1.2", "https://192.168.1.2/clip/v2", "192.168.1.2"},
		{"192.168.1.2:8443", "https://192.168.1.2:8443/clip/v2", "192.168.1.2"},
		{"bridge.local", "https://bridge.local/clip/v2", "bridge.local"},
		{"fe80::1", "https://[fe80::1]/clip/v2", "fe80::1"},
		{"2001:db8::5", "https://[2001:db8::5]/clip/v2", "2001:db8::5"},
		{"[fe80::1]", "https://[fe80::1]/clip/v2", "fe80::1"},
		{"[fe80::1]:8443", "https://[fe80::1]:8443/clip/v2", "fe80::1"},
	} {
		got := New(Options{Address: tc.addr}).url("/clip/v2")
		if got != tc.wantURL {
			t.Errorf("url for %q = %q, want %q", tc.addr, got, tc.wantURL)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Errorf("url for %q does not parse: %v", tc.addr, err)
			continue
		}
		if u.Hostname() != tc.wantHost {
			t.Errorf("url for %q resolves to host %q, want %q", tc.addr, u.Hostname(), tc.wantHost)
		}
	}
}

// mdnsMessage builds a DNS message with an optional question section and one A
// record per address. flags goes straight into the header, so a caller can
// clear the QR bit to produce a query rather than a reply.
func mdnsMessage(t *testing.T, flags uint16, question string, qtype uint16, ips ...string) []byte {
	t.Helper()
	var body bytes.Buffer
	var qd uint16
	if question != "" {
		if err := encodeName(&body, question); err != nil {
			t.Fatalf("encode question %q: %v", question, err)
		}
		var tail [4]byte
		binary.BigEndian.PutUint16(tail[0:2], qtype)
		binary.BigEndian.PutUint16(tail[2:4], 1) // class IN
		body.Write(tail[:])
		qd = 1
	}
	for _, ip := range ips {
		if err := encodeName(&body, "philips-hue.local"); err != nil {
			t.Fatalf("encode record name: %v", err)
		}
		var rr [10]byte
		binary.BigEndian.PutUint16(rr[0:2], dnsTypeA)
		binary.BigEndian.PutUint16(rr[2:4], 1)   // class IN
		binary.BigEndian.PutUint32(rr[4:8], 120) // ttl
		binary.BigEndian.PutUint16(rr[8:10], 4)  // rdlength
		body.Write(rr[:])
		body.Write(net.ParseIP(ip).To4())
	}
	hdr := make([]byte, 12)
	binary.BigEndian.PutUint16(hdr[2:4], flags)
	binary.BigEndian.PutUint16(hdr[4:6], qd)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(ips)))
	return append(hdr, body.Bytes()...)
}

// mdnsResponse is QR set with the authoritative-answer bit, as a responder
// sends it.
const mdnsResponse = 0x8400

// TestParseARecordsChecksWhatItIsAnswering. The parser used to harvest A
// records out of any datagram that reached the ephemeral port, including one
// that was not a reply at all. Every candidate is still only a candidate, since
// Probe decides. But a stranger's answer about some other service has no
// business being probed.
func TestParseARecordsChecksWhatItIsAnswering(t *testing.T) {
	got := parseARecords(mdnsMessage(t, mdnsResponse, mdnsService, dnsTypePTR, "192.168.1.7"))
	if len(got) != 1 || got[0] != "192.168.1.7" {
		t.Fatalf("a reply to our own question was dropped: %v", got)
	}
	if got := parseARecords(mdnsMessage(t, mdnsResponse, "_workstation._tcp.local", dnsTypePTR, "192.168.1.7")); got != nil {
		t.Errorf("a reply about another service was harvested: %v", got)
	}
	if got := parseARecords(mdnsMessage(t, 0, mdnsService, dnsTypePTR, "192.168.1.7")); got != nil {
		t.Errorf("someone else's query was harvested as an answer: %v", got)
	}
	// A responder is entitled to answer without echoing the question back, and
	// rejecting that would break discovery against bridges that do.
	if got := parseARecords(mdnsMessage(t, mdnsResponse, "", 0, "192.168.1.7")); len(got) != 1 {
		t.Errorf("a question-less response was dropped: %v", got)
	}
}

func listenUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Skipf("no loopback UDP socket available: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestCollectMDNSStopsOnCancellation. The read loop honored ctx.Deadline but
// not cancellation, so Ctrl-C during `discover` sat out the whole window.
func TestCollectMDNSStopsOnCancellation(t *testing.T) {
	conn := listenUDP(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	// A listen window far longer than the test will wait, so only the
	// cancellation can end this.
	_, err := collectMDNS(ctx, conn, time.Now().Add(time.Minute))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("collectMDNS returned %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("sat in the read for %s after the context was canceled", elapsed)
	}
}

// TestCollectMDNSReportsSocketErrors. A socket that failed and a window that
// simply closed both used to end the loop silently, so Discover reported "no
// Hue bridge found" for a network stack that had refused to play.
func TestCollectMDNSReportsSocketErrors(t *testing.T) {
	conn := listenUDP(t)
	_ = conn.Close()

	_, err := collectMDNS(context.Background(), conn, time.Now().Add(time.Minute))
	if err == nil {
		t.Fatal("a dead socket must not look like an empty network")
	}
	if !strings.Contains(err.Error(), "mdns read") {
		t.Errorf("socket failure was not reported as one: %v", err)
	}
}

// TestCollectMDNSTreatsTheDeadlineAsNormal. The window closing is how this
// finishes, not a failure. Reporting it would put noise into the "nothing
// found" message every time.
func TestCollectMDNSTreatsTheDeadlineAsNormal(t *testing.T) {
	addrs, err := collectMDNS(context.Background(), listenUDP(t), time.Now().Add(20*time.Millisecond))
	if err != nil {
		t.Fatalf("the listen window closing is not an error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("nothing answered, yet we collected %v", addrs)
	}
}

// TestCollectMDNSHarvestsAReply is the other half of the validation. The checks
// must not turn away an answer that really is ours.
func TestCollectMDNSHarvestsAReply(t *testing.T) {
	conn := listenUDP(t)
	peer := listenUDP(t)
	reply := mdnsMessage(t, mdnsResponse, mdnsService, dnsTypePTR, "192.168.1.7", "192.168.1.9")
	if _, err := peer.WriteToUDP(reply, conn.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Skipf("cannot send on loopback: %v", err)
	}

	addrs, err := collectMDNS(context.Background(), conn, time.Now().Add(300*time.Millisecond))
	if err != nil {
		t.Fatalf("collectMDNS: %v", err)
	}
	if len(addrs) != 2 || addrs[0] != "192.168.1.7" || addrs[1] != "192.168.1.9" {
		t.Fatalf("collected %v, want both addresses from the reply", addrs)
	}
}

// TestPairKeepsPollingThroughTransientFailures. The pairing window is the two
// minutes the user spends standing at the bridge, so a 503 from a busy bridge
// is the likeliest error there is. It used to abort the ceremony and cost them
// another button press.
func TestPairKeepsPollingThroughTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch attempts.Add(1) {
		case 1:
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case 2:
			_, _ = w.Write([]byte(`[{"error":{"type":101,"description":"link button not pressed"}}]`))
		default:
			_, _ = w.Write([]byte(`[{"success":{"username":"app-key"}}]`))
		}
	}))
	t.Cleanup(srv.Close)

	c := New(Options{BaseURL: srv.URL, Insecure: true, RequestsPerSecond: 100})
	key, err := c.PairWithRetry(context.Background(), "test", time.Millisecond, nil)
	if err != nil {
		t.Fatalf("pairing gave up on a transient failure: %v", err)
	}
	if key != "app-key" {
		t.Errorf("got app key %q, want app-key", key)
	}
}

// TestPairStopsOnARefusalTheBridgeSpelledOut. Polling past a definite "no"
// would just burn the window and end with the wrong explanation.
func TestPairStopsOnARefusalTheBridgeSpelledOut(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"error":{"type":7,"description":"invalid value for parameter devicetype"}}]`))
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := New(Options{BaseURL: srv.URL, Insecure: true, RequestsPerSecond: 100})
	start := time.Now()
	_, err := c.PairWithRetry(ctx, "test", time.Millisecond, nil)
	if err == nil {
		t.Fatal("a refusal the bridge spelled out must end pairing")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("kept polling for %s after a permanent refusal", elapsed)
	}
	if !strings.Contains(err.Error(), "invalid value for parameter devicetype") {
		t.Errorf("the bridge's description was lost: %v", err)
	}
}

// TestPairReportsWhyItGaveUp. After two minutes of an unreachable bridge, the
// user should not be told they failed to press a button.
func TestPairReportsWhyItGaveUp(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c := New(Options{BaseURL: srv.URL, Insecure: true, RequestsPerSecond: 100})
	_, err := c.PairWithRetry(ctx, "test", 10*time.Millisecond, nil)
	if err == nil {
		t.Fatal("expected pairing to run out of time")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("the give-up message should name the bridge's last answer: %v", err)
	}
}
