package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// shell returns a shell to run trivial commands with, or skips: these tests
// are about run and probe, not about any particular platform's service manager.
func shell(t *testing.T) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh on this machine")
	}
	return sh
}

func TestRunEchoesTheCommandAndItsOutput(t *testing.T) {
	sh := shell(t)
	var out bytes.Buffer
	if err := run(context.Background(), &out, sh, "-c", "echo hello"); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	// The echoed command is the point: it is what the user repeats by hand.
	if !strings.Contains(got, "+ "+sh+" -c echo hello") {
		t.Errorf("command was not echoed, got %q", got)
	}
	if !strings.Contains(got, "hello") {
		t.Errorf("command output was not forwarded, got %q", got)
	}
}

func TestRunReportsAFailingCommand(t *testing.T) {
	sh := shell(t)
	var out bytes.Buffer
	err := run(context.Background(), &out, sh, "-c", "exit 3")
	if err == nil {
		t.Fatal("a command that exited 3 was reported as success")
	}
	// The message has to name the command; "exit status 3" alone is useless
	// when start ran three of them.
	if !strings.Contains(err.Error(), "exit 3") {
		t.Errorf("error does not name the command: %v", err)
	}
}

func TestProbeReportsExitStatus(t *testing.T) {
	sh := shell(t)
	ctx := context.Background()
	if !probe(ctx, sh, "-c", "exit 0") {
		t.Error("probe said a successful command failed")
	}
	if probe(ctx, sh, "-c", "exit 1") {
		t.Error("probe said a failing command succeeded")
	}
	if probe(ctx, "definitely-not-a-real-binary-6f3a") {
		t.Error("probe said a missing binary succeeded")
	}
}

// TestParseExecStart feeds the parser the shape `systemctl show -p ExecStart
// --value` actually prints for the shipped unit, captured rather than
// imagined, the same way TestLaunchdField pins launchctl's output.
func TestParseExecStart(t *testing.T) {
	show := "{ path=/usr/local/bin/hue-scene-keeper ; argv[]=/usr/local/bin/hue-scene-keeper " +
		"--config /etc/hue-scene-keeper/config.yaml --state /var/lib/hue-scene-keeper/credentials.json run ; " +
		"ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
	config, state := parseExecStart(show)
	if config != "/etc/hue-scene-keeper/config.yaml" {
		t.Errorf("config = %q", config)
	}
	if state != "/var/lib/hue-scene-keeper/credentials.json" {
		t.Errorf("state = %q", state)
	}
}

func TestParseExecStartHandlesTheEqualsForm(t *testing.T) {
	config, state := parseExecStart("{ path=/x ; argv[]=/x --config=/a.yaml --state=/b.json run ; pid=0 }")
	if config != "/a.yaml" || state != "/b.json" {
		t.Errorf("got %q, %q", config, state)
	}
}

func TestParseExecStartReturnsNothingForAUnitWithoutFlags(t *testing.T) {
	for _, show := range []string{
		"",
		"{ path=/usr/local/bin/hue-scene-keeper ; argv[]=/usr/local/bin/hue-scene-keeper run ; pid=0 }",
		"not systemctl output at all",
	} {
		if config, state := parseExecStart(show); config != "" || state != "" {
			t.Errorf("parseExecStart(%q) = %q, %q; want empty", show, config, state)
		}
	}
}

// TestPrintPathAnnotatesAMissingFile pins the annotation contract: a definite
// absence is called out, an existing file is printed bare, and the label
// column stays aligned with the rest of status's hand-padded output.
func TestPrintPathAnnotatesAMissingFile(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(present, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	printPath(&out, "config:", present, "")
	printPath(&out, "", filepath.Join(dir, "gone.json"), "systemd unit")

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines:\n%s", len(lines), out.String())
	}
	if want := "config:    " + present; lines[0] != want {
		t.Errorf("present file line = %q, want %q", lines[0], want)
	}
	if want := "           " + filepath.Join(dir, "gone.json") + " (systemd unit, not present)"; lines[1] != want {
		t.Errorf("missing file line = %q, want %q", lines[1], want)
	}
}

// TestStatusReportsNotRunningAsAnExitStatus pins the half of status a script
// reads. The prose is for a person; a monitoring check or an `if` in a shell
// script has only the exit status to go on, and a status command that always
// succeeds cannot answer the question it was asked.
//
// It runs against whatever this machine's service manager actually says, so
// which branch it takes depends on the machine - but both branches are
// assertions, not skips.
func TestStatusReportsNotRunningAsAnExitStatus(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("no service manager to ask on " + runtime.GOOS)
	}
	var out bytes.Buffer
	dir := t.TempDir()
	err := Status(context.Background(), &out, Paths{
		Config: filepath.Join(dir, "config.yaml"),
		State:  filepath.Join(dir, "credentials.json"),
	})

	// The human-readable verdict is what tells the two cases apart, and
	// keeping it is the point: the exit status is an addition to it, not a
	// replacement for it.
	switch {
	case strings.Contains(out.String(), "recalling scenes right now"):
		if err != nil {
			t.Fatalf("status of a running service returned %v, want nil", err)
		}
	case strings.Contains(out.String(), "Nothing is recalling scenes"):
		if err == nil {
			t.Fatal("status of a stopped service returned nil; a script cannot tell it from a running one")
		}
		var coded interface{ ExitStatus() int }
		if !errors.As(err, &coded) {
			t.Fatalf("status returned %v, which carries no exit status", err)
		}
		// LSB reserves 3 for "program is not running".
		if got := coded.ExitStatus(); got != 3 {
			t.Errorf("exit status %d, want 3", got)
		}
	default:
		t.Fatalf("status printed no verdict at all:\n%s", out.String())
	}
}
