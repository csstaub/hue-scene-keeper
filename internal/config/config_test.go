package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/hue"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadMissingFileUsesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("a missing config should not be an error: %v", err)
	}
	if cfg.SceneName != DefaultSceneName {
		t.Errorf("scene name = %q", cfg.SceneName)
	}
	if cfg.MinRecallInterval.Duration() != MinRecallFloor {
		t.Errorf("min recall interval = %s", cfg.MinRecallInterval.Duration())
	}
}

func TestLoadParsesDurations(t *testing.T) {
	path := writeConfig(t, `
scene_name: "Natürliches Licht"
recall_cooldown: 7s
coalesce_window: 250ms
min_recall_interval: 30s
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SceneName != "Natürliches Licht" {
		t.Errorf("scene name = %q", cfg.SceneName)
	}
	if cfg.RecallCooldown.Duration() != 7*time.Second {
		t.Errorf("cooldown = %s", cfg.RecallCooldown.Duration())
	}
	if cfg.CoalesceWindow.Duration() != 250*time.Millisecond {
		t.Errorf("coalesce = %s", cfg.CoalesceWindow.Duration())
	}
	if cfg.MinRecallInterval.Duration() != 30*time.Second {
		t.Errorf("min interval = %s", cfg.MinRecallInterval.Duration())
	}
}

func TestBareSecondsAreAccepted(t *testing.T) {
	cfg, err := Load(writeConfig(t, "recall_cooldown: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RecallCooldown.Duration() != 3*time.Second {
		t.Fatalf("cooldown = %s", cfg.RecallCooldown.Duration())
	}
}

// TestMinRecallIntervalCannotGoBelowFloor. The floor is the last defense
// against a recall/event feedback loop, so it is not configurable away.
func TestMinRecallIntervalCannotGoBelowFloor(t *testing.T) {
	cfg, err := Load(writeConfig(t, "min_recall_interval: 1s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinRecallInterval.Duration() != MinRecallFloor {
		t.Fatalf("floor not enforced: got %s, want %s", cfg.MinRecallInterval.Duration(), MinRecallFloor)
	}
}

func TestInvalidDurationIsRejected(t *testing.T) {
	if _, err := Load(writeConfig(t, "recall_cooldown: banana\n")); err == nil {
		t.Fatal("expected an error for an unparsable duration")
	}
}

// fakeLookup is a stand-in for the registry. shadowed names the groups whose
// lights all resolve elsewhere, which on a real bridge is every zone over live
// rooms. shadowedFn, when set, answers instead and sees the exclusion
// predicate, for the tests where shadowing depends on what is excluded.
type fakeLookup struct {
	lights     []hue.Light
	rooms      []hue.Group
	zones      []hue.Group
	shadowed   map[string]bool
	shadowedFn func(groupID string, excluded func(string) bool) bool
}

func (f fakeLookup) Lights() []hue.Light { return f.lights }
func (f fakeLookup) Rooms() []hue.Group  { return f.rooms }
func (f fakeLookup) Zones() []hue.Group  { return f.zones }

func (f fakeLookup) GroupShadowed(groupID string, excluded func(string) bool) bool {
	if f.shadowedFn != nil {
		return f.shadowedFn(groupID, excluded)
	}
	return f.shadowed[groupID]
}

func testLookup() fakeLookup {
	return fakeLookup{
		lights: []hue.Light{
			{ID: "l1", Metadata: &hue.Metadata{Name: "Ceiling"}},
			{ID: "l2", Metadata: &hue.Metadata{Name: "Night Stand"}},
		},
		rooms: []hue.Group{{ID: "r1", Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: "Bedroom"}}},
		zones: []hue.Group{{ID: "z1", Type: hue.TypeZone, Metadata: &hue.Metadata{Name: "Downstairs"}}},
	}
}

func TestResolveExclusionsByNameAndID(t *testing.T) {
	cfg := Default()
	cfg.Exclude.Lights = []string{"night stand", "l1"}
	cfg.Exclude.Rooms = []string{"BEDROOM", "z1"}

	ex := ResolveExclusions(cfg, testLookup())

	for _, id := range []string{"l1", "l2"} {
		if !ex.LightExcluded(id) {
			t.Errorf("light %s should be excluded", id)
		}
	}
	for _, id := range []string{"r1", "z1"} {
		if !ex.GroupExcluded(id) {
			t.Errorf("group %s should be excluded", id)
		}
	}
	if len(ex.Unmatched) != 0 {
		t.Errorf("unexpected unmatched entries: %v", ex.Unmatched)
	}
}

// TestUnmatchedExclusionsAreReported keeps a typo from silently doing nothing.
func TestUnmatchedExclusionsAreReported(t *testing.T) {
	cfg := Default()
	cfg.Exclude.Rooms = []string{"Bedrom"}
	cfg.Exclude.Lights = []string{"Ceilling"}

	ex := ResolveExclusions(cfg, testLookup())

	if len(ex.Unmatched) != 2 {
		t.Fatalf("expected both typos reported, got %v", ex.Unmatched)
	}
	if ex.GroupExcluded("r1") || ex.LightExcluded("l1") {
		t.Error("a typo must not exclude anything")
	}
}

func TestEmptyExclusionsExcludeNothing(t *testing.T) {
	ex := ResolveExclusions(Default(), testLookup())
	if ex.LightExcluded("l1") || ex.GroupExcluded("r1") {
		t.Error("nothing should be excluded by default")
	}
	var nilEx *Exclusions
	if nilEx.LightExcluded("l1") || nilEx.GroupExcluded("r1") {
		t.Error("a nil Exclusions must be safe and permissive")
	}
}

func TestSmartSceneOverrideMatchesNameOrID(t *testing.T) {
	cfg := Default()
	cfg.SmartSceneOverrides = map[string]string{"bedroom": "scene-x"}

	group := hue.Group{ID: "r1", Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: "Bedroom"}}
	if id, ok := cfg.SmartSceneOverride(group); !ok || id != "scene-x" {
		t.Fatalf("override by name failed: %q %v", id, ok)
	}

	cfg.SmartSceneOverrides = map[string]string{"r1": "scene-y"}
	if id, ok := cfg.SmartSceneOverride(group); !ok || id != "scene-y" {
		t.Fatalf("override by id failed: %q %v", id, ok)
	}

	other := hue.Group{ID: "r2", Metadata: &hue.Metadata{Name: "Kitchen"}}
	if _, ok := cfg.SmartSceneOverride(other); ok {
		t.Error("override matched the wrong group")
	}
}

func TestCredentialsRoundTripWithTightPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "credentials.json")
	creds, err := LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	creds.AppKey = "secret"
	creds.CertPin = "pin"
	creds.Address = "192.0.2.10"
	if err := creds.Save(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("credentials should be 0600, got %o", perm)
	}

	reloaded, err := LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.AppKey != "secret" || reloaded.CertPin != "pin" {
		t.Fatalf("round trip lost data: %+v", reloaded)
	}
}

// TestIneffectiveExclusionsAreReported covers the zone case. The name matches,
// so nothing lands in Unmatched. But rooms win in GroupForLight and the
// exclusion does nothing. Silence there is worse than the typo warnings this
// package already emits, because the config looks right.
func TestIneffectiveExclusionsAreReported(t *testing.T) {
	tests := []struct {
		name            string
		rooms           []string
		shadowed        map[string]bool
		wantUnmatched   int
		wantIneffective int
		wantExcluded    string
	}{
		{
			name:            "shadowed zone",
			rooms:           []string{"Downstairs"},
			shadowed:        map[string]bool{"z1": true},
			wantIneffective: 1,
			wantExcluded:    "z1",
		},
		{
			name:         "room is never shadowed",
			rooms:        []string{"Bedroom"},
			wantExcluded: "r1",
		},
		{
			name:         "zone that no room shadows still works",
			rooms:        []string{"z1"},
			wantExcluded: "z1",
		},
		{
			// A name shared by a room and a zone excludes both. The room half
			// works, so there is nothing to warn about.
			name:         "one usable match is enough",
			rooms:        []string{"Shared"},
			shadowed:     map[string]bool{"z2": true},
			wantExcluded: "r2",
		},
		{
			// A typo is Unmatched, never Ineffective. The two diagnostics say
			// different things and must not be conflated.
			name:          "typo stays unmatched",
			rooms:         []string{"Downstars"},
			shadowed:      map[string]bool{"z1": true},
			wantUnmatched: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			look := testLookup()
			look.shadowed = tt.shadowed
			look.rooms = append(look.rooms, hue.Group{ID: "r2", Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: "Shared"}})
			look.zones = append(look.zones, hue.Group{ID: "z2", Type: hue.TypeZone, Metadata: &hue.Metadata{Name: "Shared"}})

			cfg := Default()
			cfg.Exclude.Rooms = tt.rooms
			ex := ResolveExclusions(cfg, look)

			if len(ex.Unmatched) != tt.wantUnmatched {
				t.Errorf("Unmatched = %v, want %d entries", ex.Unmatched, tt.wantUnmatched)
			}
			if len(ex.Ineffective) != tt.wantIneffective {
				t.Errorf("Ineffective = %v, want %d entries", ex.Ineffective, tt.wantIneffective)
			}
			if tt.wantExcluded != "" && !ex.GroupExcluded(tt.wantExcluded) {
				t.Errorf("group %s should still be excluded", tt.wantExcluded)
			}
		})
	}
}

// TestIneffectiveExclusionSeesTheWholeList. Effectiveness is judged under the
// complete exclusion set, with each group's own entry peeled off. A zone that
// is shadowed only while its room is live stops being ineffective the moment
// the same config excludes that room. That is the carve-out pattern. A group is
// also never judged under its own exclusion, which would find the entire list
// ineffective.
func TestIneffectiveExclusionSeesTheWholeList(t *testing.T) {
	look := testLookup()
	look.shadowedFn = func(groupID string, excluded func(string) bool) bool {
		if excluded != nil && excluded(groupID) {
			t.Errorf("group %s judged under its own exclusion", groupID)
		}
		// The zone carves lights out of the room, so it is shadowed only
		// while the room is live.
		return groupID == "z1" && (excluded == nil || !excluded("r1"))
	}

	cfg := Default()
	cfg.Exclude.Rooms = []string{"Downstairs"}
	if ex := ResolveExclusions(cfg, look); len(ex.Ineffective) != 1 {
		t.Errorf("zone alone: Ineffective = %v, want 1 entry", ex.Ineffective)
	}

	cfg.Exclude.Rooms = []string{"Downstairs", "Bedroom"}
	ex := ResolveExclusions(cfg, look)
	if len(ex.Ineffective) != 0 {
		t.Errorf("zone plus its room: Ineffective = %v, want none", ex.Ineffective)
	}
	if !ex.GroupExcluded("z1") || !ex.GroupExcluded("r1") {
		t.Error("both groups should be excluded")
	}
}

// TestIneffectiveExclusionMentionsTheEntry. The warning is only useful if the
// user can find the line it is about.
func TestIneffectiveExclusionMentionsTheEntry(t *testing.T) {
	look := testLookup()
	look.shadowed = map[string]bool{"z1": true}

	cfg := Default()
	cfg.Exclude.Rooms = []string{"downstairs"}
	ex := ResolveExclusions(cfg, look)

	if len(ex.Ineffective) != 1 {
		t.Fatalf("Ineffective = %v", ex.Ineffective)
	}
	for _, want := range []string{"exclude.rooms", "downstairs", `zone "Downstairs"`} {
		if !strings.Contains(ex.Ineffective[0], want) {
			t.Errorf("diagnostic %q does not mention %q", ex.Ineffective[0], want)
		}
	}
}

// TestOverrideKeysAreResolved. A misspelled override key used to fall through
// to name matching with no diagnostic at all.
func TestOverrideKeysAreResolved(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		want      int
		mentions  []string
	}{
		{
			name:      "matched by name",
			overrides: map[string]string{"Bedroom": "scene-x"},
		},
		{
			name:      "matched by id",
			overrides: map[string]string{"r1": "scene-x"},
		},
		{
			name:      "matched by zone name",
			overrides: map[string]string{"downstairs": "scene-x"},
		},
		{
			name:      "typo",
			overrides: map[string]string{"Bedrom": "scene-x"},
			want:      1,
			mentions:  []string{"smart_scene_overrides", "Bedrom"},
		},
		{
			name:      "id and name naming different scenes",
			overrides: map[string]string{"r1": "scene-x", "Bedroom": "scene-y"},
			want:      1,
			mentions:  []string{`"r1"`, `"Bedroom"`, `room "Bedroom"`, "scene-x"},
		},
		{
			name:      "id and name naming the same scene",
			overrides: map[string]string{"r1": "scene-x", "Bedroom": "scene-x"},
		},
		{
			name:      "one good key, one typo",
			overrides: map[string]string{"Bedroom": "scene-x", "Ktichen": "scene-y"},
			want:      1,
			mentions:  []string{"Ktichen"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.SmartSceneOverrides = tt.overrides
			ex := ResolveExclusions(cfg, testLookup())

			if len(ex.OverrideProblems) != tt.want {
				t.Fatalf("OverrideProblems = %v, want %d entries", ex.OverrideProblems, tt.want)
			}
			for _, want := range tt.mentions {
				if !strings.Contains(ex.OverrideProblems[0], want) {
					t.Errorf("diagnostic %q does not mention %q", ex.OverrideProblems[0], want)
				}
			}
		})
	}
}

// TestOverrideConflictResolvesToTheIDDeterministically. Map iteration order is
// randomized per range, so a coin-flip winner shows up within a few rounds.
func TestOverrideConflictResolvesToTheID(t *testing.T) {
	cfg := Default()
	cfg.SmartSceneOverrides = map[string]string{"r1": "scene-by-id", "Bedroom": "scene-by-name"}
	group := hue.Group{ID: "r1", Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: "Bedroom"}}

	for i := 0; i < 200; i++ {
		id, ok := cfg.SmartSceneOverride(group)
		if !ok || id != "scene-by-id" {
			t.Fatalf("round %d: got %q (ok=%v), want the id match to win every time", i, id, ok)
		}
	}
}

// TestOverrideTiesBreakOnTheKey. Two keys that are both names can only collide
// across different groups. The tiebreak must still be stable if they ever do.
func TestOverrideTiesBreakOnTheKey(t *testing.T) {
	cfg := Default()
	cfg.SmartSceneOverrides = map[string]string{"bedroom": "scene-a", "r1": "scene-b"}
	// A group whose name and id both hit a key, neither being an id match.
	group := hue.Group{ID: "R1", Metadata: &hue.Metadata{Name: "Bedroom"}}
	for i := 0; i < 200; i++ {
		if id, _ := cfg.SmartSceneOverride(group); id != "scene-b" {
			t.Fatalf("round %d: got %q, want the id match", i, id)
		}
	}
}

// TestDuplicateOverrideKeysAreALoadError. Two keys that normalize the same are
// unambiguously a mistake, and spotting them needs no bridge.
func TestDuplicateOverrideKeysAreALoadError(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name:    "differing case",
			body:    "smart_scene_overrides:\n  Kitchen: scene-a\n  kitchen: scene-b\n",
			wantErr: true,
		},
		{
			name:    "surrounding whitespace",
			body:    "smart_scene_overrides:\n  \"Kitchen \": scene-a\n  Kitchen: scene-b\n",
			wantErr: true,
		},
		{
			name: "genuinely different keys",
			body: "smart_scene_overrides:\n  Kitchen: scene-a\n  Bedroom: scene-b\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.body))
			if tt.wantErr && err == nil {
				t.Fatal("expected a load error for duplicate keys")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "smart_scene_overrides") {
				t.Errorf("error should name the offending section: %v", err)
			}
		})
	}
}

func TestNewTuningKeysDefaultAndParse(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CoalesceMax.Duration() != 3*time.Second {
		t.Errorf("default coalesce_max = %s", cfg.CoalesceMax.Duration())
	}
	if cfg.Bridge.RequestsPerSecond != DefaultRequestsPerSecond {
		t.Errorf("default requests_per_second = %g", cfg.Bridge.RequestsPerSecond)
	}
	if cfg.Bridge.RequestTimeout.Duration() != hue.DefaultRequestTimeout {
		t.Errorf("default request_timeout = %s", cfg.Bridge.RequestTimeout.Duration())
	}

	cfg, err = Load(writeConfig(t, `
coalesce_max: 8s
bridge:
  requests_per_second: 2
  request_timeout: 30s
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CoalesceMax.Duration() != 8*time.Second {
		t.Errorf("coalesce_max = %s", cfg.CoalesceMax.Duration())
	}
	if cfg.Bridge.RequestsPerSecond != 2 {
		t.Errorf("requests_per_second = %g", cfg.Bridge.RequestsPerSecond)
	}
	if cfg.Bridge.RequestTimeout.Duration() != 30*time.Second {
		t.Errorf("request_timeout = %s", cfg.Bridge.RequestTimeout.Duration())
	}
}

// TestLogLevelIsNamedOrAbsent covers the three states log.level has. An absent
// key names nothing, which is what leaves --log-level and its default in
// charge. A name is parsed here rather than downstream. A name nothing parses
// is a load error, because a level nobody understands means the debug output
// the user came here to turn on never appears with nothing said about why.
func TestLogLevelIsNamedOrAbsent(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if lvl, ok := cfg.LogLevel(); ok {
		t.Errorf("a config with no log.level names %s, want no level at all", lvl)
	}

	cfg, err = Load(writeConfig(t, "log:\n  level: debug\n"))
	if err != nil {
		t.Fatal(err)
	}
	if lvl, ok := cfg.LogLevel(); !ok || lvl != slog.LevelDebug {
		t.Errorf("LogLevel() = %s, %v, want debug, true", lvl, ok)
	}

	// Whitespace and case, because the level is a hand-edited string and both
	// slips read as the level they were meant to be.
	cfg, err = Load(writeConfig(t, "log:\n  level: \" WARN \"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if lvl, ok := cfg.LogLevel(); !ok || lvl != slog.LevelWarn {
		t.Errorf("LogLevel() = %s, %v, want warn, true", lvl, ok)
	}

	_, err = Load(writeConfig(t, "log:\n  level: verbose\n"))
	if err == nil {
		t.Fatal("log.level: verbose loaded, want an error naming the key")
	}
	if !strings.Contains(err.Error(), "log.level") {
		t.Errorf("error = %v, want it to name log.level", err)
	}
}

// TestCoalesceMaxIsRaisedToTheWindow. A cap below the gap it caps would fire
// every recall immediately, silently turning the debounce off. Raising it back
// to the window is the closest reading of what was asked for.
func TestCoalesceMaxIsRaisedToTheWindow(t *testing.T) {
	cfg, err := Load(writeConfig(t, "coalesce_window: 2s\ncoalesce_max: 500ms\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CoalesceMax.Duration() != 2*time.Second {
		t.Errorf("coalesce_max = %s, want it raised to the window", cfg.CoalesceMax.Duration())
	}
}

func TestOutOfRangeTuningIsRejected(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"rate too high", "bridge:\n  requests_per_second: 500\n", "requests_per_second"},
		{"timeout too short", "bridge:\n  request_timeout: 100ms\n", "request_timeout"},
		{"cap too long", "coalesce_max: 5m\n", "coalesce_max"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.body))
			if err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name %q: %v", tc.want, err)
			}
		})
	}
}

// TestLoadRejectsOverrideWithNoScene. `Bedroom:` with nothing after the colon
// is a common YAML slip. It reads as an override everywhere downstream, so the
// group takes the override branch, looks up scene id "", finds nothing, and is
// never recalled again. Nothing but a per-recall log line says so.
func TestLoadRejectsOverrideWithNoScene(t *testing.T) {
	for _, body := range []string{
		"smart_scene_overrides:\n  Bedroom:\n",
		"smart_scene_overrides:\n  Bedroom: \"\"\n",
		"smart_scene_overrides:\n  Bedroom: \"   \"\n",
	} {
		if _, err := Load(writeConfig(t, body)); err == nil {
			t.Errorf("an override with no scene must be rejected at load time: %q", body)
		}
	}
}

// TestResolveOverridesReportsOneKeyMatchingSeveralGroups. A smart scene belongs
// to one group, so a key matching two same-named rooms pins a scene that only
// one of them can use. The other fails the scene-owns-this-group check at
// recall time and is silently never recalled.
func TestResolveOverridesReportsOneKeyMatchingSeveralGroups(t *testing.T) {
	look := testLookup()
	look.rooms = append(look.rooms, hue.Group{
		ID: "r2", Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: "Bedroom"},
	})

	cfg := Default()
	cfg.SmartSceneOverrides = map[string]string{"Bedroom": "scene-x"}

	ex := ResolveExclusions(cfg, look)
	if len(ex.OverrideProblems) == 0 {
		t.Fatal("one key matching two groups must be reported")
	}
	if !strings.Contains(ex.OverrideProblems[0], "Bedroom") {
		t.Errorf("the problem should name the key, got %q", ex.OverrideProblems[0])
	}
}

// TestLoadRejectsAbsurdRecallTimers. A bare number means seconds, so someone
// thinking in milliseconds gets ten minutes from `min_recall_interval: 600`
// and a daemon that looks broken. Every other knob is range-checked. These two
// only had a floor.
func TestLoadRejectsAbsurdRecallTimers(t *testing.T) {
	for _, body := range []string{
		"min_recall_interval: 6000\n", // meant as milliseconds
		"recall_cooldown: 7200\n",
		"min_recall_interval: 2h\n",
		// Out of range. Converting this to a Duration saturates on arm64 but
		// wraps negative on amd64, so the same config meant two things.
		"min_recall_interval: 1e300\n",
	} {
		if _, err := Load(writeConfig(t, body)); err == nil {
			t.Errorf("expected a range error for %q", body)
		}
	}
}

// TestNaNRequestsPerSecondFallsBackToTheDefault. NaN satisfies neither the
// <= 0 check nor the ceiling. Without an explicit test it survived both and the
// derived limiter interval came out zero, meaning no rate limiting at all.
func TestNaNRequestsPerSecondFallsBackToTheDefault(t *testing.T) {
	cfg, err := Load(writeConfig(t, "bridge:\n  requests_per_second: .nan\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Bridge.RequestsPerSecond; got != DefaultRequestsPerSecond {
		t.Fatalf("want the default %g, got %g", float64(DefaultRequestsPerSecond), got)
	}
}

// TestNegativeTuningIsRejected. Every guard used to be `<= 0`, so a negative
// was quietly replaced by the default and the daemon ran on numbers the config
// never asked for. Zero cannot be told from absent without pointer fields. A
// negative is unambiguously a mistake, and this package rejects those.
func TestNegativeTuningIsRejected(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{"recall_cooldown: -5s\n", "recall_cooldown"},
		{"min_recall_interval: -1m\n", "min_recall_interval"},
		{"coalesce_window: -300ms\n", "coalesce_window"},
		{"coalesce_max: -3s\n", "coalesce_max"},
		{"bridge:\n  request_timeout: -10s\n", "bridge.request_timeout"},
		{"bridge:\n  requests_per_second: -4\n", "bridge.requests_per_second"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.body))
			if err == nil {
				t.Fatalf("a negative %s must be rejected", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name %q: %v", tc.want, err)
			}
		})
	}
}

// TestZeroTuningTakesTheDefault is the other half of the split. `0` still means
// "default", not "off". Nothing here can distinguish a key written as 0 from
// one that is absent, so the two must behave the same.
func TestZeroTuningTakesTheDefault(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
recall_cooldown: 0
min_recall_interval: 0
coalesce_window: 0
coalesce_max: 0
bridge:
  requests_per_second: 0
  request_timeout: 0
`))
	if err != nil {
		t.Fatalf("zero should fill in the defaults, not fail: %v", err)
	}
	def := Default()
	if cfg.RecallCooldown != def.RecallCooldown {
		t.Errorf("recall_cooldown = %s", cfg.RecallCooldown.Duration())
	}
	if cfg.MinRecallInterval.Duration() != MinRecallFloor {
		t.Errorf("min_recall_interval = %s", cfg.MinRecallInterval.Duration())
	}
	if cfg.CoalesceWindow != def.CoalesceWindow {
		t.Errorf("coalesce_window = %s", cfg.CoalesceWindow.Duration())
	}
	if cfg.CoalesceMax != def.CoalesceMax {
		t.Errorf("coalesce_max = %s", cfg.CoalesceMax.Duration())
	}
	if cfg.Bridge.RequestsPerSecond != DefaultRequestsPerSecond {
		t.Errorf("requests_per_second = %g", cfg.Bridge.RequestsPerSecond)
	}
	if cfg.Bridge.RequestTimeout.Duration() != hue.DefaultRequestTimeout {
		t.Errorf("request_timeout = %s", cfg.Bridge.RequestTimeout.Duration())
	}
}

// TestSecondYAMLDocumentIsRejected. A decoder reads one document, so a stray
// `---` turned the whole rest of the file into settings that did nothing at
// all. That is the silent no-op KnownFields exists to prevent, one granularity
// up.
func TestSecondYAMLDocumentIsRejected(t *testing.T) {
	_, err := Load(writeConfig(t, "scene_name: a\n---\nscene_name: b\n"))
	if err == nil {
		t.Fatal("a second document must be an error, not silently dropped")
	}
	if !strings.Contains(err.Error(), "document") {
		t.Errorf("error should explain what was ignored: %v", err)
	}

	// A single document that merely starts with the marker is ordinary YAML.
	cfg, err := Load(writeConfig(t, "---\nscene_name: a\n"))
	if err != nil {
		t.Fatalf("a leading --- is one document: %v", err)
	}
	if cfg.SceneName != "a" {
		t.Errorf("scene name = %q", cfg.SceneName)
	}
}

// TestCleanAddressRejectsBadHostsAndPorts. Everything CleanAddress lets past is
// spliced straight into "https://" + addr. Anything it fails to catch comes
// back as a net/url error naming no config key, which is the "baffling errors
// much later" the function exists to prevent.
func TestCleanAddressRejectsBadHostsAndPorts(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
		want string // empty means "expect an error"
	}{
		{name: "plain ipv4", addr: "192.168.1.42", want: "192.168.1.42"},
		{name: "ipv4 with port", addr: "192.168.1.42:443", want: "192.168.1.42:443"},
		{name: "hostname", addr: "hue-bridge.local", want: "hue-bridge.local"},
		{name: "bracketed ipv6 with port", addr: "[2001:db8::1]:443", want: "[2001:db8::1]:443"},
		{name: "bare ipv6 is left bare", addr: "2001:db8::1", want: "2001:db8::1"},
		{name: "bare ipv6 with no port", addr: "::1", want: "::1"},
		{name: "bracketed ipv6 without a port", addr: "[2001:db8::1]", want: "[2001:db8::1]"},
		{name: "words", addr: "hello world"},
		{name: "port too high", addr: "192.168.1.42:99999"},
		{name: "port zero", addr: "192.168.1.42:0"},
		{name: "negative port", addr: "192.168.1.42:-1"},
		{name: "empty port", addr: "192.168.1.42:"},
		{name: "hostname with a space and a port", addr: "my bridge:443"},
		{name: "empty label", addr: "hue..local"},
		{name: "trailing dash", addr: "hue-.local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CleanAddress(tc.addr)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("CleanAddress(%q) = %q, want an error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("CleanAddress(%q): %v", tc.addr, err)
			}
			if got != tc.want {
				t.Errorf("CleanAddress(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}

// TestBlankExclusionEntriesAreReported. A blank entry protects nothing. It used
// to be dropped before the matching loop, so it reached neither Unmatched nor
// Ineffective. In the one package that reports every inert config line, that
// was the exception.
func TestBlankExclusionEntriesAreReported(t *testing.T) {
	cfg := Default()
	cfg.Exclude.Rooms = []string{"  "}
	cfg.Exclude.Lights = []string{""}

	ex := ResolveExclusions(cfg, testLookup())

	if len(ex.Unmatched) != 2 {
		t.Fatalf("both blank entries should be reported, got %v", ex.Unmatched)
	}
	for _, want := range []string{"exclude.lights", "exclude.rooms"} {
		found := false
		for _, entry := range ex.Unmatched {
			if strings.Contains(entry, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("no diagnostic for %s in %v", want, ex.Unmatched)
		}
	}
}

// TestLooseCredentialsPermissionsAreWarnedAbout. Save is meticulous about 0600.
// But a file restored from a backup or written by an older version can arrive
// at 0644, and loading the bridge's application key out of a world-readable
// file in silence is the one thing this package would not say.
func TestLooseCredentialsPermissionsAreWarnedAbout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(`{"app_key":"secret"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	var logged strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	creds, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("a loose mode is a warning, not a failure: %v", err)
	}
	if creds.AppKey != "secret" {
		t.Fatalf("app key = %q", creds.AppKey)
	}
	if !strings.Contains(logged.String(), "readable by other users") {
		t.Fatalf("0644 credentials should be warned about, log was %q", logged.String())
	}

	logged.Reset()
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCredentials(path); err != nil {
		t.Fatal(err)
	}
	if logged.Len() != 0 {
		t.Errorf("a 0600 file must be silent, got %q", logged.String())
	}
}

// CleanAddress has to accept its own output. auth persists the cleaned address
// into credentials.json and resolveAddress cleans it again on every start, so a
// value the daemon wrote itself must survive the round trip. Bracketing IPv6
// here as well as in hue.hostPort broke exactly that.
func TestCleanAddressAcceptsItsOwnOutput(t *testing.T) {
	for _, in := range []string{
		"2001:db8::1", "[2001:db8::1]", "[2001:db8::1]:443", "::1",
		"192.168.1.42", "192.168.1.42:443", "bridge.local", "bridge.local:8443",
	} {
		once, err := CleanAddress(in)
		if err != nil {
			t.Errorf("CleanAddress(%q) = error %v; want it accepted", in, err)
			continue
		}
		twice, err := CleanAddress(once)
		if err != nil {
			t.Errorf("CleanAddress(%q) = %q, which is then rejected: %v", in, once, err)
			continue
		}
		if twice != once {
			t.Errorf("CleanAddress is not idempotent for %q: %q then %q", in, once, twice)
		}
	}
}

// TestExampleConfigSelectsNothing pins the example file's whole purpose. It is
// a commented reference, and installing it is meant to change nothing.
//
// The systemd unit passes --config explicitly, and an explicit path that does
// not exist is fatal, so a Linux install has to put a file there (`go tool mage
// installConfig`). That file is this one. Leave a value in it uncommented and
// that setting freezes at whatever this version's default happened to be. A
// later release changing the default would then silently not reach anyone who
// installed before it. Nothing else would report that, which is why this test
// exists.
//
// The stamped copy is what installConfig actually writes: the same bytes under
// two lines of provenance. Those live in the magefile, which carries the mage
// build tag and so cannot be tested directly. Prepending an equivalent header
// here covers the only thing about them that could break parsing.
func TestExampleConfigSelectsNothing(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	stamp := []byte("# Installed by hue-scene-keeper v1.2.3 on 2026-01-02T03:04:05Z.\n" +
		"# Written once; no upgrade rewrites it. Edit it freely.\n\n")

	for name, body := range map[string][]byte{
		"as shipped":                 raw,
		"as installConfig writes it": append(stamp, raw...),
	} {
		cfg, err := Load(writeConfig(t, string(body)))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !reflect.DeepEqual(cfg, Default()) {
			t.Errorf("%s: the example config is not the defaults:\n got %+v\nwant %+v", name, cfg, Default())
		}
	}
}
