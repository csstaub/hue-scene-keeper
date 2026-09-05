package config

import (
	"os"
	"path/filepath"
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

// TestMinRecallIntervalCannotGoBelowFloor: the floor is the last defence
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
// lights all resolve elsewhere, which on a real bridge is every zone.
type fakeLookup struct {
	lights   []hue.Light
	rooms    []hue.Group
	zones    []hue.Group
	shadowed map[string]bool
}

func (f fakeLookup) Lights() []hue.Light { return f.lights }
func (f fakeLookup) Rooms() []hue.Group  { return f.rooms }
func (f fakeLookup) Zones() []hue.Group  { return f.zones }

func (f fakeLookup) GroupShadowed(groupID string) bool { return f.shadowed[groupID] }

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

// TestIneffectiveExclusionsAreReported covers the zone case: the name matches,
// so nothing lands in Unmatched, yet rooms win in GroupForLight and the
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
			// A typo is Unmatched, never Ineffective: the two diagnostics say
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

// TestIneffectiveExclusionMentionsTheEntry: the warning is only useful if the
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

// TestOverrideKeysAreResolved: a misspelled override key used to fall through
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

// TestOverrideConflictResolvesToTheIDDeterministically: map iteration order is
// randomised per range, so a coin-flip winner shows up within a few rounds.
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

// TestOverrideTiesBreakOnTheKey: two name-shaped keys can only collide across
// different groups, but the tiebreak must still be stable if they ever do.
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

// TestDuplicateOverrideKeysAreALoadError: two keys that normalise the same are
// unambiguously a mistake, and need no bridge to spot.
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

// TestCoalesceMaxIsRaisedToTheWindow: a cap below the gap it caps would fire
// every recall immediately, silently turning the debounce off. Raising it back
// to the window is the closest honest reading of what was asked for.
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

// TestLoadRejectsOverrideWithNoScene: `Bedroom:` with nothing after the colon
// is a common YAML slip. It reads as an override everywhere downstream, so the
// group takes the override branch, looks up scene id "", finds nothing, and is
// never recalled again - with nothing but a per-recall log line to say so.
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

// TestResolveOverridesReportsOneKeyMatchingSeveralGroups: a smart scene belongs
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
