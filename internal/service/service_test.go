package service

import (
	"bytes"
	"context"
	"os/exec"
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
