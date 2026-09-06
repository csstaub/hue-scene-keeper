// Package service starts and stops the installed background service through
// the platform's own service manager - launchd on macOS, systemd on Linux.
//
// It deliberately does not signal the daemon process. Both managers are
// configured to resurrect what they supervise (KeepAlive, Restart=always), so
// a signal is undone within seconds; and whether the daemon comes back at the
// next login or boot is a fact the manager owns, not something this process
// can decide. Asking the manager is the only way to make "stopped" stick.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"strings"
)

const (
	// binName is the installed executable, and the stem of the systemd unit.
	binName = "hue-scene-keeper"

	// Label is the launchd job label. It must match the Label key in
	// deploy/dev.staub.hue-scene-keeper.plist.
	Label = "dev.staub." + binName
)

// run executes one command, echoing it first. Showing the exact invocation
// matters more here than in most places: these commands change state the
// daemon itself cannot see, and a user who wants to check or undo one by hand
// needs to know precisely what was run.
func run(ctx context.Context, out io.Writer, argv ...string) error {
	_, _ = fmt.Fprintln(out, "+", strings.Join(argv, " "))
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	// Inherited so sudo can prompt for a password on Linux.
	cmd.Stdin = os.Stdin
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", strings.Join(argv, " "), err)
	}
	return nil
}

// capture runs a command for its standard output, which is how the service
// managers answer questions: launchctl and systemctl both have machine-shaped
// query subcommands, and reading those beats inferring state from a process
// listing. Output is returned even when the command fails, because both tools
// report "not running" through a non-zero exit.
func capture(ctx context.Context, argv ...string) (string, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	out, err := cmd.Output()
	return string(out), err
}

// verdict answers the question status is usually asked for: whether something
// in the background is about to move the lights, which is what makes a
// `--dry-run` session look like it is lying.
func verdict(running bool) string {
	if running {
		return "\nA background daemon is recalling scenes right now.\nStop it with `" + binName + " stop` before running with --dry-run."
	}
	return "\nNothing is recalling scenes in the background."
}

// Paths carries the config and credentials paths the CLI resolved from its
// flags and XDG environment. Status prints them as candidates rather than
// facts: the service manager runs the daemon with its own environment, which
// on Linux resolves different paths entirely, so the reader is being handed a
// list of possibilities, not a promise.
type Paths struct {
	Config string
	State  string
}

// printPath prints one aligned path line. The label column is the same
// hand-aligned 11 characters as the rest of status's output; a continuation
// line under the previous label passes label "".
//
// A path whose file is missing is annotated, but only on a definite
// ErrNotExist: a permission error (a root-owned state directory read as a
// user) proves nothing about the file and gets no annotation.
func printPath(w io.Writer, label, path, note string) {
	var notes []string
	if note != "" {
		notes = append(notes, note)
	}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		notes = append(notes, "not present")
	}
	suffix := ""
	if len(notes) > 0 {
		suffix = " (" + strings.Join(notes, ", ") + ")"
	}
	_, _ = fmt.Fprintf(w, "%-11s%s%s\n", label, path, suffix)
}

// parseExecStart pulls the unit's own --config and --state values out of
// `systemctl show -p ExecStart --value` output, which looks like
//
//	{ path=/usr/local/bin/hue-scene-keeper ; argv[]=/usr/local/bin/hue-scene-keeper --config /etc/... --state /var/... run ; ignore_errors=no ; ... }
//
// The argv segment is tokenized on spaces, so a path containing one is lost -
// the shipped unit's paths never do, and a failed parse returns empty strings,
// which the caller reads as "no extra candidates" rather than an error.
func parseExecStart(show string) (config, state string) {
	_, argv, ok := strings.Cut(show, "argv[]=")
	if !ok {
		return "", ""
	}
	if end := strings.Index(argv, " ; "); end >= 0 {
		argv = argv[:end]
	}
	fields := strings.Fields(argv)
	for i, f := range fields {
		next := ""
		if i+1 < len(fields) {
			next = fields[i+1]
		}
		switch {
		case f == "--config":
			config = next
		case f == "--state":
			state = next
		case strings.HasPrefix(f, "--config="):
			config = strings.TrimPrefix(f, "--config=")
		case strings.HasPrefix(f, "--state="):
			state = strings.TrimPrefix(f, "--state=")
		}
	}
	return config, state
}

// probe runs a command for its exit status alone, discarding both streams. It
// is how the platform files ask the service manager what it currently thinks,
// rather than guessing from a pid file or a process listing.
func probe(ctx context.Context, argv ...string) bool {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	return cmd.Run() == nil
}
