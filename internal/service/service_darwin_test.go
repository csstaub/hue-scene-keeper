//go:build darwin

package service

import "testing"

// Real output from `launchctl list dev.staub.hue-scene-keeper` while the agent
// was running. The format is a plist-ish dictionary, not JSON, so the parser
// is worth pinning to a sample launchd actually produced.
const listOutput = `{
	"LimitLoadToSessionType" = "Aqua";
	"Label" = "dev.staub.hue-scene-keeper";
	"OnDemand" = false;
	"LastExitStatus" = 0;
	"PID" = 94644;
	"Program" = "/bin/sh";
	"ProgramArguments" = (
		"/bin/sh";
		"-c";
		"mkdir -p \"$HOME/Library/Logs\" && exec /usr/local/bin/hue-scene-keeper run";
	);
};`

func TestLaunchdField(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"PID", "94644"},
		{"LastExitStatus", "0"},
		{"Label", "dev.staub.hue-scene-keeper"},
		{"OnDemand", "false"},
		// A stopped job has no PID key at all; an empty answer is how Status
		// tells "loaded but idle" from "running".
		{"NoSuchKey", ""},
	} {
		if got := launchdField(listOutput, tc.key); got != tc.want {
			t.Errorf("launchdField(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestLaunchdFieldIgnoresAnAbsentDictionary(t *testing.T) {
	// What capture returns when the job is not loaded: launchctl writes its
	// complaint to stderr and nothing to stdout.
	if got := launchdField("", "PID"); got != "" {
		t.Errorf("launchdField on empty output = %q, want empty", got)
	}
}
