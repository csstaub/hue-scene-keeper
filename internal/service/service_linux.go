//go:build linux

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// unit is the systemd unit, as named by deploy/hue-scene-keeper.service.
const unit = binName + ".service"

// systemd owns both halves of the question here: `stop` halts the unit for now,
// `disable` decides whether it returns at boot. The commands below always do
// both, so that start and stop mean the same thing on Linux as they do on
// macOS - the daemon is running, or it is not, across reboots either way.

// invocation is how to run systemctl: the base argv, including --user and
// any sudo.
type invocation struct {
	base []string
	user bool
}

// detect asks systemctl which unit it knows rather than stat-ing the several
// directories a unit can be installed into. The system unit is the one the
// repo ships, so it wins; a user unit is a reasonable hand install.
func detect(ctx context.Context) (invocation, error) {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return invocation{}, errors.New("systemctl not found; this machine does not run systemd, " +
			"so start and stop have nothing to drive - run `" + binName + " run` under whatever supervises services here")
	}
	if probe(ctx, "systemctl", "cat", unit) {
		base := []string{"systemctl"}
		// A system unit needs root. Going through sudo beats failing with a
		// permission error the user then has to translate back into a command.
		if os.Geteuid() != 0 {
			if sudo, err := exec.LookPath("sudo"); err == nil {
				base = append([]string{sudo}, base...)
			}
		}
		return invocation{base: base}, nil
	}
	if probe(ctx, "systemctl", "--user", "cat", unit) {
		return invocation{base: []string{"systemctl", "--user"}, user: true}, nil
	}
	return invocation{}, fmt.Errorf("systemd knows no unit named %s; install deploy/%s "+
		"into /etc/systemd/system and run `systemctl daemon-reload` (see the README)", unit, unit)
}

func (s invocation) where() string {
	if s.user {
		return "user"
	}
	return "system"
}

// Start enables the unit and starts it now.
func Start(ctx context.Context, out io.Writer) error {
	s, err := detect(ctx)
	if err != nil {
		return err
	}
	if err := run(ctx, out, append(append([]string{}, s.base...), "enable", "--now", unit)...); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "started: %s unit %s, and it will start again at boot\n", s.where(), unit)
	return nil
}

// Restart restarts the unit, or starts it if it was stopped, and makes sure it
// comes back at boot either way. The enable is what keeps restart's outcome
// identical to start's. `systemctl restart` alone would happily bounce a
// disabled unit into a state where it runs now and silently stays down after
// the next reboot. That is neither of the two states start and stop promise.
func Restart(ctx context.Context, out io.Writer) error {
	s, err := detect(ctx)
	if err != nil {
		return err
	}
	if err := run(ctx, out, append(append([]string{}, s.base...), "enable", unit)...); err != nil {
		return err
	}
	if err := run(ctx, out, append(append([]string{}, s.base...), "restart", unit)...); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "restarted: %s unit %s, and it will start again at boot\n", s.where(), unit)
	return nil
}

// Stop stops the unit and keeps it from coming back at boot.
func Stop(ctx context.Context, out io.Writer) error {
	s, err := detect(ctx)
	if err != nil {
		return err
	}
	if err := run(ctx, out, append(append([]string{}, s.base...), "disable", "--now", unit)...); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "stopped: %s unit %s, and it will stay stopped across reboots until `%s start`\n",
		s.where(), unit, binName)
	return nil
}

// notRunningError is how Status answers a script rather than a person: main
// turns it into exit status 3 - the LSB code for "the program is not running"
// - and prints nothing for it, because Status has already said so in words.
type notRunningError struct{}

func (notRunningError) Error() string   { return binName + " is not running" }
func (notRunningError) ExitStatus() int { return 3 }

// Status reports whether the unit is running, and whether it will come back at
// the next boot - the two halves of "is anything else about to move my lights?".
//
// It returns notRunningError when nothing is running, so `status` can be used
// in a monitoring check without parsing its prose.
func Status(ctx context.Context, out io.Writer, p Paths) error {
	s, err := detect(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(out, "service:   none installed\n           %v\n", err)
		printPath(out, "config:", p.Config, "")
		printPath(out, "creds:", p.State, "")
		_, _ = fmt.Fprintln(out, verdict(false))
		// No unit at all is still "not running": a check asking whether
		// anything is about to move the lights wants the same answer.
		return notRunningError{}
	}
	_, _ = fmt.Fprintf(out, "service:   systemd %s unit %s\n", s.where(), unit)

	// is-active, is-enabled and show are systemd's own machine-readable
	// queries; all three report through stdout, and is-active/is-enabled also
	// through a non-zero exit that carries no extra information here.
	active := s.query(ctx, "is-active", unit)
	enabled := s.query(ctx, "is-enabled", unit)
	pid := s.query(ctx, "show", "--property=MainPID", "--value", unit)
	running := active == "active"

	if running && pid != "" && pid != "0" {
		_, _ = fmt.Fprintf(out, "state:     running (pid %s)\n", pid)
	} else {
		_, _ = fmt.Fprintf(out, "state:     %s\n", orUnknown(active))
	}

	switch enabled {
	case "enabled", "enabled-runtime", "static", "indirect":
		_, _ = fmt.Fprintf(out, "at boot:   %s - it starts on its own\n", enabled)
	default:
		_, _ = fmt.Fprintf(out, "at boot:   %s - it will not start on its own until `%s start`\n",
			orUnknown(enabled), binName)
	}

	// The unit passes its own --config/--state (the shipped one uses /etc and
	// /var/lib), which are usually not what this shell resolves. Both are
	// candidates; the reader knows which process they are asking about.
	unitConfig, unitState := parseExecStart(s.query(ctx, "show", "--property=ExecStart", "--value", unit))
	printPath(out, "config:", p.Config, "")
	if unitConfig != "" && unitConfig != p.Config {
		printPath(out, "", unitConfig, "systemd unit")
	}
	printPath(out, "creds:", p.State, "")
	if unitState != "" && unitState != p.State {
		printPath(out, "", unitState, "systemd unit")
	}
	journalctl := "journalctl -u " + unit
	if s.user {
		journalctl = "journalctl --user -u " + unit
	}
	_, _ = fmt.Fprintf(out, "logs:      journald (%s -f)\n", journalctl)

	_, _ = fmt.Fprintln(out, verdict(running))
	if !running {
		return notRunningError{}
	}
	return nil
}

// query runs one read-only systemctl subcommand and returns its trimmed
// output. The exit status is ignored: `is-active` exits non-zero precisely
// when it has the answer "inactive" to report.
//
// It drops any sudo prefix. These queries need no privileges, and prompting
// for a password to answer a question would be a poor trade.
func (s invocation) query(ctx context.Context, args ...string) string {
	base := s.base
	if len(base) > 0 && filepath.Base(base[0]) == "sudo" {
		base = base[1:]
	}
	out, _ := capture(ctx, append(append([]string{}, base...), args...)...)
	return strings.TrimSpace(out)
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
