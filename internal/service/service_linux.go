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

// scope is the systemctl invocation to use, including --user and any sudo.
type scope struct {
	base []string
	user bool
}

// detect asks systemctl which unit it knows rather than stat-ing the several
// directories a unit can be installed into. The system unit is the one the
// repo ships, so it wins; a user unit is a reasonable hand install.
func detect(ctx context.Context) (scope, error) {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return scope{}, errors.New("systemctl not found; this machine does not run systemd, " +
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
		return scope{base: base}, nil
	}
	if probe(ctx, "systemctl", "--user", "cat", unit) {
		return scope{base: []string{"systemctl", "--user"}, user: true}, nil
	}
	return scope{}, fmt.Errorf("systemd knows no unit named %s; install deploy/%s "+
		"into /etc/systemd/system and run `systemctl daemon-reload` (see the README)", unit, unit)
}

func (s scope) where() string {
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

// Status reports whether the unit is running, and whether it will come back at
// the next boot - the two halves of "is anything else about to move my lights?".
func Status(ctx context.Context, out io.Writer) error {
	s, err := detect(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(out, "service:   none installed\n           %v\n", err)
		_, _ = fmt.Fprintln(out, verdict(false))
		return nil
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

	_, _ = fmt.Fprintln(out, verdict(running))
	return nil
}

// query runs one read-only systemctl subcommand and returns its trimmed
// output. The exit status is ignored: `is-active` exits non-zero precisely
// when it has the answer "inactive" to report.
//
// It drops any sudo prefix. These queries need no privileges, and prompting
// for a password to answer a question would be a poor trade.
func (s scope) query(ctx context.Context, args ...string) string {
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
