package hue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// selfSigned returns the DER bytes of a bridge-like certificate and the pin
// value that certificate should produce.
func selfSigned(t *testing.T) (der []byte, pin string) {
	t.Helper()
	cert, pin := selfSignedCert(t)
	return cert.Certificate[0], pin
}

// TestPinLearnsOnFirstContact: trust-on-first-use. The bridge is self-signed,
// so the first certificate seen is the one we commit to.
func TestPinLearnsOnFirstContact(t *testing.T) {
	der, want := selfSigned(t)
	p := NewPin("")
	if err := p.verify([][]byte{der}, nil); err != nil {
		t.Fatalf("first contact should be trusted: %v", err)
	}
	if got := p.Value(); got != want {
		t.Fatalf("pin = %q, want %q", got, want)
	}
}

// TestPinRejectsADifferentCertificate: the whole point of pinning. A different
// key on the same address is either a replaced bridge or something pretending
// to be one, and either way it must not be trusted silently.
func TestPinRejectsADifferentCertificate(t *testing.T) {
	_, first := selfSigned(t)
	other, _ := selfSigned(t)

	p := NewPin(first)
	err := p.verify([][]byte{other}, nil)
	if !errors.Is(err, ErrPinMismatch) {
		t.Fatalf("want ErrPinMismatch, got %v", err)
	}
	if p.Value() != first {
		t.Fatal("a rejected certificate must not replace the stored pin")
	}
	if Retryable(err) {
		t.Fatal("a pin mismatch never resolves itself, so it must not be retryable")
	}
}

// TestPinFailsClosed: an empty chain or an unparseable certificate must be an
// error, never a silent pass. These are the paths where failing open would
// disable pinning altogether.
func TestPinFailsClosed(t *testing.T) {
	t.Run("no certificate", func(t *testing.T) {
		if err := NewPin("").verify(nil, nil); err == nil {
			t.Fatal("an empty chain must be rejected")
		}
	})
	t.Run("unparseable certificate", func(t *testing.T) {
		p := NewPin("")
		if err := p.verify([][]byte{{0x01, 0x02, 0x03}}, nil); err == nil {
			t.Fatal("a certificate that does not parse must be rejected")
		}
		if p.Value() != "" {
			t.Fatal("nothing should have been learned")
		}
	})
}

// TestPinRefusesToTrustWhatItCannotPersist: if OnLearn fails and we kept the
// pin in memory anyway, this process would carry on while nothing was written,
// so every restart would silently re-enter the trust-on-first-use window.
func TestPinRefusesToTrustWhatItCannotPersist(t *testing.T) {
	der, _ := selfSigned(t)
	p := NewPin("")
	p.OnLearn = func(string) error { return errors.New("disk full") }

	if err := p.verify([][]byte{der}, nil); err == nil {
		t.Fatal("a pin that could not be persisted must not be trusted")
	}
	if p.Value() != "" {
		t.Fatal("the pin must not be committed in memory when the save failed")
	}
}

// TestPinLearnsOnlyOnce: concurrent handshakes serialise on the mutex, so
// exactly one of them learns and the rest verify against it.
func TestPinLearnsOnlyOnce(t *testing.T) {
	der, want := selfSigned(t)
	var learned int
	p := NewPin("")
	p.OnLearn = func(string) error { learned++; return nil }

	for range 3 {
		if err := p.verify([][]byte{der}, nil); err != nil {
			t.Fatalf("verify: %v", err)
		}
	}
	if learned != 1 {
		t.Fatalf("OnLearn should fire once, fired %d times", learned)
	}
	if p.Value() != want {
		t.Fatal("wrong pin stored")
	}
}

// TestPinDoesNotHoldItsLockAcrossOnLearn: OnLearn runs from inside the TLS
// handshake, and it is where the pin is written to disk. Calling it under the
// same lock Value() takes meant any callback that read its own Pin - to log
// what it was about to store, or to compare it with what it already had -
// wedged the handshake goroutine for good.
func TestPinDoesNotHoldItsLockAcrossOnLearn(t *testing.T) {
	der, want := selfSigned(t)
	p := NewPin("")
	var duringLearn string
	p.OnLearn = func(string) error {
		duringLearn = p.Value()
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- p.verify([][]byte{der}, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnLearn deadlocked against the pin's own lock")
	}
	if duringLearn != "" {
		t.Fatalf("the pin was committed before it was persisted: %q", duringLearn)
	}
	if p.Value() != want {
		t.Fatal("wrong pin stored")
	}
}

// TestPinLearnsOnlyOnceUnderConcurrentHandshakes: the value's own mutex is no
// longer what serialises learning, so the guarantee needs a test that arrives
// concurrently. Several connections opening at once is the ordinary case - the
// daemon opens the event stream and its first resource GETs together - and
// every extra OnLearn is another fsync'd write of the same value.
func TestPinLearnsOnlyOnceUnderConcurrentHandshakes(t *testing.T) {
	der, want := selfSigned(t)
	var learned atomic.Int64
	p := NewPin("")
	p.OnLearn = func(string) error {
		learned.Add(1)
		// Wide enough that another handshake would be well inside the window
		// if nothing held it out.
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- p.verify([][]byte{der}, nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
	}
	if n := learned.Load(); n != 1 {
		t.Fatalf("OnLearn should fire once, fired %d times", n)
	}
	if p.Value() != want {
		t.Fatal("wrong pin stored")
	}
}
