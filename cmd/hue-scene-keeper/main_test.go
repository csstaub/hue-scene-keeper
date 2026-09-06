package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/config"
	"github.com/csstaub/hue-scene-keeper/internal/hue"
	"github.com/csstaub/hue-scene-keeper/internal/hue/fake"
)

// daemonEnv, when set, turns this test binary into the daemon: TestMain hands
// its own command line to main() instead of running any tests.
//
// Signals are the whole subject here - what a SIGTERM finishes before it exits,
// and what a second one cuts short - and neither can be observed from inside
// the process being signaled. Re-executing the test binary is the standard way
// to get a real process, with a real signal disposition, to send them to. The
// arguments ride on argv rather than in the environment, so the daemon parses
// exactly what a shell would have handed it; no test flag is ever parsed,
// because this returns before m.Run.
const daemonEnv = "HUE_SCENE_KEEPER_TEST_DAEMON"

func TestMain(m *testing.M) {
	if os.Getenv(daemonEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// syncBuffer collects the daemon's log output. exec copies it on a goroutine of
// its own, so a test reading it while the daemon still runs needs the lock.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// daemon is one running child process and the log it is writing.
type daemon struct {
	cmd *exec.Cmd
	log *syncBuffer
	// done carries the single Wait. Reaping the child exactly once, on a
	// goroutine started with it, keeps the test and its cleanup from calling
	// Wait against each other.
	done   chan error
	reaped bool
}

// startDaemon runs `hue-scene-keeper run` against b as a real child process,
// paired in advance and pointed at a config of the test's choosing.
func startDaemon(t *testing.T, b *fake.Bridge, configBody string) *daemon {
	t.Helper()
	dir := t.TempDir()

	statePath := filepath.Join(dir, "credentials.json")
	// No cert_pin: the daemon learns the fake's certificate on first contact
	// and writes it back here, which is the path `auth` shares.
	creds := []byte(`{"app_key": "test-key"}` + "\n")
	if err := os.WriteFile(statePath, creds, 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	args := []string{
		"run",
		"--address", strings.TrimPrefix(b.URL(), "https://"),
		"--state", statePath,
		"--config", configPath,
		"--log-level", "debug",
	}
	d := &daemon{cmd: exec.Command(os.Args[0], args...), log: &syncBuffer{}, done: make(chan error, 1)}
	d.cmd.Env = append(os.Environ(), daemonEnv+"=1")
	d.cmd.Stderr = d.log
	d.cmd.Stdout = d.log
	if err := d.cmd.Start(); err != nil {
		t.Fatalf("start daemon: %v", err)
	}
	go func() { d.done <- d.cmd.Wait() }()
	t.Cleanup(func() {
		// Whatever the test did or did not manage to do to it.
		_ = d.cmd.Process.Kill()
		if !d.reaped {
			select {
			case <-d.done:
			case <-time.After(10 * time.Second):
			}
		}
		if t.Failed() {
			t.Logf("daemon log:\n%s", d.log.String())
		}
	})

	if !b.WaitForSubscriber(20 * time.Second) {
		t.Fatalf("daemon never connected to the event stream; log:\n%s", d.log.String())
	}
	// Not just connected but resynced: a light switched on during the opening
	// GET is reported by that GET as already on, so no off->on edge occurs and
	// the test would be watching for a recall nothing ever asked for.
	d.waitForLog(t, "synced bridge resources")
	return d
}

func (d *daemon) waitForLog(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(d.log.String(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("daemon never logged %q; log:\n%s", want, d.log.String())
}

func (d *daemon) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := d.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signal %s: %v", sig, err)
	}
}

// wait returns the daemon's exit status, failing the test if it is still
// running after timeout.
func (d *daemon) wait(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case err := <-d.done:
		d.reaped = true
		var exit *exec.ExitError
		switch {
		case err == nil:
			return 0
		case errors.As(err, &exit):
			return exit.ExitCode()
		default:
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("daemon did not exit within %s; log:\n%s", timeout, d.log.String())
	}
	return -1
}

// coalesceSlowly is a config whose coalescing window is long enough that a
// recall is certain to still be waiting it out when the test signals the
// daemon. Four seconds is as long as the config package will accept.
const coalesceSlowly = "coalesce_window: 4s\ncoalesce_max: 5s\n"

// settleIntoPending gives an event time to travel the stream and be folded
// into a pending recall. Nothing is logged at that point - a trigger only
// speaks up when it is acted on or refused - so this is a wait, not a poll. It
// is a quarter of the coalescing window it runs inside.
func settleIntoPending() { time.Sleep(time.Second) }

// kitchenBridge builds a fake bridge with one room and its smart scene.
func kitchenBridge(t *testing.T) (*fake.Bridge, []string) {
	t.Helper()
	b := fake.NewBridge(t)
	lights := b.AddRoom("room-kitchen", "Kitchen", "Ceiling", "Counter")
	b.AddSmartScene("scene-kitchen", config.DefaultSceneName, "room-kitchen", hue.TypeRoom)
	return b, lights
}

// TestSIGTERMSendsTheRecallItWasHolding: the drain, end to end. A
// light comes on, the recall sits out its coalescing window, and the service
// manager stops the daemon in the middle of it - a `systemctl restart` or a
// package upgrade seconds after somebody walked into the room. Nothing recovers
// that recall afterward: the light is already on, so the next start sees no
// off->on edge and leaves the room as it found it.
func TestSIGTERMSendsTheRecallItWasHolding(t *testing.T) {
	b, lights := kitchenBridge(t)
	d := startDaemon(t, b, coalesceSlowly)

	b.SwitchLight(lights[0], true)
	settleIntoPending()
	if n := len(b.Recalls()); n != 0 {
		t.Fatalf("the recall should still have been waiting out its window, got %d", n)
	}

	d.signal(t, syscall.SIGTERM)
	if code := d.wait(t, 20*time.Second); code != 0 {
		t.Fatalf("a signaled daemon should exit 0, got %d; log:\n%s", code, d.log.String())
	}
	if n := len(b.Recalls()); n != 1 {
		t.Fatalf("the recall it was holding must reach the bridge, got %d; log:\n%s", n, d.log.String())
	}
}

// TestASecondSignalQuitsMidDrain: the drain is bounded, but a bound is not the
// same as an escape. Someone watching a stop take longer than they expected
// presses Ctrl-C again, and a daemon that goes on draining through it is the
// one people learn to kill with -9.
func TestASecondSignalQuitsMidDrain(t *testing.T) {
	b, lights := kitchenBridge(t)
	// Slow enough that the drain is demonstrably still in progress when the
	// second signal lands, and still inside the grace period, so the daemon
	// would otherwise have gone on waiting for it.
	b.DelayRecalls(2500 * time.Millisecond)
	d := startDaemon(t, b, coalesceSlowly)

	b.SwitchLight(lights[0], true)
	settleIntoPending()

	d.signal(t, syscall.SIGTERM)
	waitFor(t, 5*time.Second, func() bool { return b.Attempts() > 0 })
	d.signal(t, syscall.SIGTERM)

	// 128+SIGTERM, what a shell reports for a process killed by one.
	if code := d.wait(t, 5*time.Second); code != 143 {
		t.Fatalf("a second signal should force quit with 143, got %d; log:\n%s", code, d.log.String())
	}
	if n := len(b.Recalls()); n != 0 {
		t.Fatalf("the stalled recall should have been abandoned, got %d", n)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out")
}

// TestAuthPersistsThePinAsSoonAsItLearnsIt: hue.Pin will not trust a
// certificate its OnLearn could not persist, and `auth` learns the pin on its
// first attempt - minutes before the user gets to the link button. An OnLearn
// that only set a field in memory left every abandoned pairing having trusted a
// certificate the next process knew nothing about, so it walked back into the
// trust-on-first-use window with nothing said.
func TestAuthPersistsThePinAsSoonAsItLearnsIt(t *testing.T) {
	srv := unpairedBridge(t)
	dir := t.TempDir()
	statePath := filepath.Join(dir, "credentials.json")

	g := globals{
		configPath: filepath.Join(dir, "no-such-config.yaml"),
		statePath:  statePath,
		address:    strings.TrimPrefix(srv.URL, "https://"),
	}
	// Long enough for one pairing attempt - which is where the certificate is
	// seen - and far short of the poll interval, so the link button is never
	// pressed and `auth` gives up.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := cmdAuth(ctx, g, slog.New(slog.NewTextHandler(os.Stderr, nil))); err == nil {
		t.Fatal("pairing should have failed: the link button was never pressed")
	}

	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("the pin was never written: %v", err)
	}
	var creds struct {
		CertPin string `json:"cert_pin"`
	}
	if err := json.Unmarshal(raw, &creds); err != nil {
		t.Fatalf("parse credentials: %v", err)
	}
	if creds.CertPin == "" {
		t.Fatalf("a learned pin must be on disk before it is trusted, got %s", raw)
	}
}

// unpairedBridge answers the two requests `auth` makes: the probe that
// confirms it is talking to a bridge, and a pairing attempt whose link button
// is never pressed.
func unpairedBridge(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"bridgeid":"001788FFFE123456","name":"Fake Bridge"}`)
	})
	mux.HandleFunc("/api", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `[{"error":{"type":101,"address":"","description":"link button not pressed"}}]`)
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestLogOutputWritesToTheFileItIsGiven covers the wiring between the flags
// and internal/logfile. --log-file redirects the logger off stderr, and the
// directory is made rather than demanded. A negative cap is refused even with
// no file for it to apply to, since it is a typo either way.
func TestLogOutputWritesToTheFileItIsGiven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "test.log")
	out, closeLog, err := logOutput(globals{logFile: path})
	if err != nil {
		t.Fatalf("logOutput: %v", err)
	}
	log, _, err := newLogger(out, "text", "info")
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	log.Info("hello from the daemon")
	closeLog()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(b), "hello from the daemon") {
		t.Errorf("log file = %q, want the record in it", b)
	}
}

// TestConfigCanTurnOnDebugLogging covers the half of the log level the flags
// do not: a level in the config file, applied to a logger that was built from
// the flags before any file had been read. The daemon is normally started by
// launchd or systemd, where the flags belong to a plist or a unit and the
// config file is the part a user can edit.
//
// The precedence is the point of the second half. An explicit --log-level is
// the more specific instruction and has to win, and an omitted one must not,
// even though both arrive here as the same string.
func TestConfigCanTurnOnDebugLogging(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("log:\n  level: debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "credentials.json")

	load := func(g globals) *slog.LevelVar {
		t.Helper()
		_, levelVar, err := newLogger(io.Discard, "text", g.logLevel)
		if err != nil {
			t.Fatalf("newLogger: %v", err)
		}
		g.levelVar = levelVar
		g.configPath, g.statePath = cfgPath, state
		if _, _, err := loadAll(g); err != nil {
			t.Fatalf("loadAll: %v", err)
		}
		return levelVar
	}

	if lvl := load(globals{logLevel: "info"}).Level(); lvl != slog.LevelDebug {
		t.Errorf("level = %s, want the config file's debug", lvl)
	}
	if lvl := load(globals{logLevel: "warn", logLevelSet: true}).Level(); lvl != slog.LevelWarn {
		t.Errorf("level = %s, want --log-level=warn to beat the config file", lvl)
	}
}

func TestLogOutputDefaultsToStderr(t *testing.T) {
	out, closeLog, err := logOutput(globals{})
	if err != nil {
		t.Fatalf("logOutput: %v", err)
	}
	defer closeLog()
	if out != os.Stderr {
		t.Errorf("logOutput() = %v, want os.Stderr", out)
	}
}

func TestLogOutputRejectsANegativeCap(t *testing.T) {
	if _, _, err := logOutput(globals{logMaxMB: -1}); err == nil {
		t.Error("logOutput(-1) succeeded, want an error even with no --log-file")
	}
}
