//go:build darwin

package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// launchd, in the three facts that shape this file:
//
//   - The agent runs in the console user's GUI domain, not the system domain,
//     so every target is scoped to this process's own uid. Running these
//     commands under sudo would address root's domain and do nothing useful.
//   - The plist sets KeepAlive, so `launchctl kill` is pointless - launchd
//     restarts the job at once. Booting it out of the domain is the only stop
//     that holds.
//   - A boot-out lasts until the next login, when launchd loads the agent from
//     LaunchAgents again. `disable` is the part that survives that, so stop
//     does both and start undoes both.

func domain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }

func target() string { return domain() + "/" + Label }

// plistPath finds the installed agent definition. A hand-installed agent in
// the user's own LaunchAgents wins over the one pkg:install puts in
// /Library/LaunchAgents: it is the more deliberate of the two.
func plistPath() (string, error) {
	var candidates []string
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, "Library", "LaunchAgents", Label+".plist"))
	}
	candidates = append(candidates, filepath.Join("/Library/LaunchAgents", Label+".plist"))
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no launchd agent installed (looked in %s); "+
		"install one with `go tool mage pkg:install`, or copy deploy/%s.plist "+
		"into ~/Library/LaunchAgents",
		strings.Join(candidates, " and "), Label)
}

// loaded reports whether the agent is bootstrapped into the user's GUI domain.
func loaded(ctx context.Context) bool {
	return probe(ctx, "launchctl", "print", target())
}

// Start loads the agent and makes sure it is running.
func Start(ctx context.Context, out io.Writer) error {
	plist, err := plistPath()
	if err != nil {
		return err
	}
	// Undo a previous Stop. A disabled label cannot be bootstrapped, and
	// enabling one that was never disabled is a no-op.
	if err := run(ctx, out, "launchctl", "enable", target()); err != nil {
		return err
	}
	if !loaded(ctx) {
		if err := run(ctx, out, "launchctl", "bootstrap", domain(), plist); err != nil {
			return err
		}
	}
	// RunAtLoad starts a freshly bootstrapped job, but one that was already
	// loaded and idle needs the nudge. On a running job this does nothing.
	if err := run(ctx, out, "launchctl", "kickstart", target()); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "started: launchd agent %s (%s)\n", Label, plist)
	return nil
}

// Stop stops the agent and keeps it stopped across logins.
func Stop(ctx context.Context, out io.Writer) error {
	// Disable before booting out: between the two commands the job is still
	// loaded, and disabling an already-absent job is the case launchd handles
	// least well.
	if err := run(ctx, out, "launchctl", "disable", target()); err != nil {
		return err
	}
	if !loaded(ctx) {
		_, _ = fmt.Fprintf(out, "launchd agent %s was not running\n", Label)
	} else if err := run(ctx, out, "launchctl", "bootout", target()); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "stopped: launchd agent %s, and it will stay stopped "+
		"across logins until `%s start`\n", Label, binName)
	return nil
}

// notRunningError is how Status answers a script rather than a person: main
// turns it into exit status 3 - the LSB code for "the program is not running"
// - and prints nothing for it, because Status has already said so in words.
type notRunningError struct{}

func (notRunningError) Error() string   { return binName + " is not running" }
func (notRunningError) ExitStatus() int { return 3 }

// Status reports whether the agent is running, and whether it will come back
// on its own - the two halves of "is anything else about to move my lights?".
//
// It returns notRunningError when nothing is running, so `status` can be used
// in a monitoring check without parsing its prose.
func Status(ctx context.Context, out io.Writer, p Paths) error {
	_, _ = fmt.Fprintf(out, "service:   launchd agent %s\n", Label)

	plist, err := plistPath()
	if err != nil {
		// Not an error to report on: "nothing is installed" is a status.
		_, _ = fmt.Fprintf(out, "plist:     none installed\n           %v\n", err)
	} else {
		_, _ = fmt.Fprintf(out, "plist:     %s\n", plist)
	}

	// `launchctl list <label>` fails when the job is not loaded at all, which
	// is the ordinary state after a stop rather than something to report as
	// a failure.
	info, listErr := capture(ctx, "launchctl", "list", Label)
	pid := launchdField(info, "PID")
	running := listErr == nil && pid != "" && pid != "-"

	switch {
	case running:
		_, _ = fmt.Fprintf(out, "state:     running (pid %s)\n", pid)
	case listErr == nil:
		state := "loaded but not running"
		if last := launchdField(info, "LastExitStatus"); last != "" && last != "0" {
			state += fmt.Sprintf("; last exit status %s", last)
		}
		_, _ = fmt.Fprintf(out, "state:     %s\n", state)
	default:
		_, _ = fmt.Fprintln(out, "state:     not running")
	}

	disabled, known := launchdDisabled(ctx, Label)
	switch {
	case !known:
		_, _ = fmt.Fprintln(out, "at login:  starts on its own")
	case disabled:
		_, _ = fmt.Fprintf(out, "at login:  disabled - it will not start on its own until `%s start`\n", binName)
	default:
		_, _ = fmt.Fprintln(out, "at login:  starts on its own")
	}

	// The launchd agent passes no --config or --state, so the CLI's resolved
	// paths are the agent's too - modulo an XDG_* set in a shell rc that the
	// GUI session never sees, which is exactly why they are printed.
	printPath(out, "config:", p.Config, "")
	printPath(out, "creds:", p.State, "")
	// The log path is the redirect hard-coded in the deploy plist's
	// ProgramArguments; the plist is installed verbatim, never templated.
	if home, err := os.UserHomeDir(); err == nil {
		printPath(out, "logs:", filepath.Join(home, "Library", "Logs", binName+".log"), "")
	}

	_, _ = fmt.Fprintln(out, verdict(running))
	if !running {
		return notRunningError{}
	}
	return nil
}

// launchdField pulls one value out of `launchctl list <label>`, whose output
// is a plist-ish dictionary of `"KEY" = value;` lines.
func launchdField(dict, key string) string {
	for line := range strings.SplitSeq(dict, "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok || strings.Trim(strings.TrimSpace(name), `"`) != key {
			continue
		}
		return strings.Trim(strings.TrimSpace(value), `";`)
	}
	return ""
}

// launchdDisabled reads the override database, which is what decides whether
// launchd loads the agent at the next login. A label absent from it has never
// been disabled, so known is false.
func launchdDisabled(ctx context.Context, label string) (disabled, known bool) {
	out, err := capture(ctx, "launchctl", "print-disabled", domain())
	if err != nil {
		return false, false
	}
	for line := range strings.SplitSeq(out, "\n") {
		name, value, ok := strings.Cut(line, "=>")
		if !ok || strings.Trim(strings.TrimSpace(name), `"`) != label {
			continue
		}
		// Ventura and later say enabled/disabled; older releases said
		// false/true for the same fact.
		switch strings.TrimSpace(value) {
		case "disabled", "true":
			return true, true
		default:
			return false, true
		}
	}
	return false, false
}
