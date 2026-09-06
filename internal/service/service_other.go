//go:build !darwin && !linux

package service

import (
	"context"
	"fmt"
	"io"
	"runtime"
)

// unsupported explains the gap rather than pretending to a stop that does not
// happen: without a service manager to talk to, there is nothing to stop that
// would not simply come back.
func unsupported(action string) error {
	return fmt.Errorf("%s manages the launchd agent on macOS and the systemd unit on Linux; "+
		"on %s, %s whatever supervises `%s run` here", binName, runtime.GOOS, action, binName)
}

// Status is not implemented on this platform, but the resolved paths are
// platform-independent and still worth saying before explaining the gap.
func Status(_ context.Context, out io.Writer, p Paths) error {
	printPath(out, "config:", p.Config, "")
	printPath(out, "creds:", p.State, "")
	return unsupported("check")
}

// Start is not implemented on this platform.
func Start(_ context.Context, _ io.Writer) error { return unsupported("start") }

// Stop is not implemented on this platform.
func Stop(_ context.Context, _ io.Writer) error { return unsupported("stop") }
