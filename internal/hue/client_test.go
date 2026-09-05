package hue

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestLimiterReturnsCancelledReservation guards the case where a caller gives
// up while queued: its slot has to go back, or a context that dies mid-wait
// silently delays every request behind it by a full interval - and under a
// whole-house restore those cancellations arrive in bursts.
func TestLimiterReturnsCancelledReservation(t *testing.T) {
	l := newLimiter(10) // 100ms apart

	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := l.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait returned %v, want context.Canceled", err)
	}

	// The abandoned caller reserved the slot 100ms out, so the next caller
	// should inherit it rather than queue behind it at 200ms.
	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("third wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 130*time.Millisecond {
		t.Fatalf("waited %s after a cancelled wait; the slot was not returned", elapsed)
	}
}

// TestLimiterKeepsLaterReservationOnRelease covers the concurrent case: by the
// time a cancelled caller returns its slot, someone else may have reserved
// after it. Rolling back then would pull that caller forward into a gap it is
// still waiting out, putting two requests on the wire back to back.
func TestLimiterKeepsLaterReservationOnRelease(t *testing.T) {
	l := newLimiter(10)
	mine := time.Now().Add(l.interval)
	theirs := mine.Add(l.interval)
	l.next = theirs

	l.release(mine)

	if !l.next.Equal(theirs) {
		t.Fatalf("release moved next from %v to %v; a later reservation must stand", theirs, l.next)
	}
}

// TestLimiterRejectsDeadContextWithoutReserving: a caller arriving with an
// already-cancelled context must not spend a slot on a request it will never
// send.
func TestLimiterRejectsDeadContextWithoutReserving(t *testing.T) {
	l := newLimiter(10)
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

// TestRetryableIgnoresTheCallersContext: a shutting-down daemon has nobody
// left to retry for, but its own per-request deadline expiring means the
// bridge was slow, which is worth another go.
func TestRetryableIgnoresTheCallersContext(t *testing.T) {
	if Retryable(context.Canceled) {
		t.Error("a cancelled caller must not be retried")
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

// TestRequestTimeoutFiresAndIsDistinguishable: hue.Client has no
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

// TestRequestTimeoutStartsAfterTheRateLimiter: the deadline covers the request,
// not the wait for a turn. Starting it at entry would time out requests that
// were never sent, and the deeper the queue the more of them.
func TestRequestTimeoutStartsAfterTheRateLimiter(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"rid":"scene-1","rtype":"smart_scene"}]}`))
	}))
	t.Cleanup(srv.Close)

	// One request per second, so the second call waits a second for its turn -
	// far longer than the timeout it must not spend while queued.
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
