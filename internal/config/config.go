// Package config loads hue-scene-keeper's YAML configuration and resolves the
// exclusion lists against the bridge's actual rooms and lights.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/csstaub/hue-scene-keeper/internal/hue"
)

// DefaultSceneName is the Hue app's English name for the all-day smart scene.
const DefaultSceneName = "Natural Light"

// MinRecallFloor is the smallest permitted interval between recalls of the same
// group. It is deliberately not configurable below this value: it is the last
// line of defence against a recall/event feedback loop hammering the bridge.
const MinRecallFloor = 10 * time.Second

// DefaultRequestsPerSecond is the outbound request budget shared by every call
// to the bridge.
const DefaultRequestsPerSecond = 4

// MaxRequestsPerSecond is the highest rate we will let a config ask for. The
// bridge's own guidance is far below this; anything higher is a mistake that
// would show up as the bridge refusing requests rather than as extra speed.
const MaxRequestsPerSecond = 20

// MaxRecallInterval is the longest either recall timer may be set to. It is a
// sanity ceiling, not a tuning limit: anything approaching it means the value
// was meant as milliseconds.
const MaxRecallInterval = time.Hour

// Duration is a time.Duration that accepts "5s"-style YAML strings.
type Duration time.Duration

// UnmarshalYAML parses either a duration string ("5s") or a bare number of
// seconds. Dispatch is on the node tag: yaml happily decodes a bare 3 into the
// string "3", so trying the string form first would swallow the numeric case.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	switch node.Tag {
	case "!!int", "!!float":
		var secs float64
		if err := node.Decode(&secs); err != nil {
			return fmt.Errorf("invalid duration at line %d: %w", node.Line, err)
		}
		// Range-check before the conversion. Converting an out-of-range float
		// to time.Duration is implementation-defined: 1e300 saturates on
		// arm64 but wraps to the negative minimum on amd64, where a later
		// floor clamp would quietly turn it into the smallest legal value. The
		// same config would then mean two different things on two release
		// targets.
		ns := secs * float64(time.Second)
		if math.IsNaN(ns) || math.IsInf(ns, 0) || ns > math.MaxInt64 || ns < math.MinInt64 {
			return fmt.Errorf("duration %g seconds at line %d is out of range", secs, node.Line)
		}
		*d = Duration(time.Duration(ns))
		return nil
	default:
		var s string
		if err := node.Decode(&s); err != nil {
			return fmt.Errorf("invalid duration at line %d: %w", node.Line, err)
		}
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(parsed)
		return nil
	}
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Config is the on-disk configuration.
type Config struct {
	Bridge struct {
		Address string `yaml:"address"`

		// RequestsPerSecond caps every outbound request to the bridge:
		// recalls, resyncs and power-restore lookups share one budget. Lower
		// it if the bridge starts refusing requests under load.
		RequestsPerSecond float64 `yaml:"requests_per_second"`

		// RequestTimeout bounds one request, measured from the moment the
		// rate limiter releases it rather than from when it was queued.
		RequestTimeout Duration `yaml:"request_timeout"`
	} `yaml:"bridge"`

	// SceneName is the smart scene to recall. Configurable because the Hue
	// app localises it.
	SceneName string `yaml:"scene_name"`

	// ApplyOnStartup recalls every room that already has a light on when the
	// daemon starts. Off by default so a restart does not restyle a room you
	// deliberately set.
	ApplyOnStartup bool `yaml:"apply_on_startup"`

	// RecallCooldown is how long triggers from a group are ignored after it
	// is recalled. This absorbs the wave of on-events our own recall causes.
	RecallCooldown Duration `yaml:"recall_cooldown"`

	// MinRecallInterval is the hard floor between recalls of the same group.
	MinRecallInterval Duration `yaml:"min_recall_interval"`

	// CoalesceWindow is how quiet a group must be before it is recalled. Any
	// activity on one of its lights restarts the wait, so this is an idle
	// gap rather than a fixed window: a single switch flip is acted on after
	// one gap, while a home automation writing to the whole house is allowed
	// to finish first.
	CoalesceWindow Duration `yaml:"coalesce_window"`

	// CoalesceMax caps how far CoalesceWindow can push a recall out. Without
	// it a light that chatters - a dynamic scene, a flaky radio - would defer
	// its group's recall indefinitely.
	CoalesceMax Duration `yaml:"coalesce_max"`

	// SmartSceneOverrides maps a room or zone (name or id) to an explicit
	// smart scene id, bypassing name matching.
	SmartSceneOverrides map[string]string `yaml:"smart_scene_overrides"`

	Exclude struct {
		// Rooms are never recalled; also matches zones. Excluding a room
		// additionally hands its lights to any non-excluded zone that holds
		// them, which is how a zone carves a light out of a room: the zone's
		// own smart scene then governs that light, and roommates in no zone
		// are left entirely alone.
		Rooms []string `yaml:"rooms"`
		// Lights never *trigger* a recall. They are still lit by a recall
		// caused by another light in the same room.
		Lights []string `yaml:"lights"`
	} `yaml:"exclude"`
}

// Default returns the configuration used when no file exists.
func Default() *Config {
	c := &Config{
		SceneName:         DefaultSceneName,
		RecallCooldown:    Duration(5 * time.Second),
		MinRecallInterval: Duration(MinRecallFloor),
		CoalesceWindow:    Duration(300 * time.Millisecond),
		CoalesceMax:       Duration(3 * time.Second),
	}
	c.Bridge.RequestsPerSecond = DefaultRequestsPerSecond
	c.Bridge.RequestTimeout = Duration(hue.DefaultRequestTimeout)
	return c
}

// Load reads a config file, applying defaults for anything absent. A missing
// file is not an error: the zero configuration is a working one.
func Load(path string) (*Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	// KnownFields, so a misspelled key is an error rather than a silent
	// no-op. Getting `romes:` instead of `rooms:` wrong would otherwise
	// quietly disable an exclusion the user thinks is protecting a room.
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return cfg, nil // empty file
		}
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	// A stream of documents decodes only its first one, so everything after a
	// `---` would be a whole file of settings that silently does nothing -
	// KnownFields' failure mode at a larger granularity. There is no reading of
	// a second document we could honour, so it is an error rather than a merge.
	var extra yaml.Node
	switch err := dec.Decode(&extra); {
	case err == nil:
		return nil, fmt.Errorf("parse %s: line %d starts a second YAML document; "+
			"only the first is read, so everything after the `---` would be ignored", path, extra.Line)
	case !errors.Is(err, io.EOF):
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyDefaults fills in anything left at zero.
//
// Zero means "default", never "off". There are no pointer fields here, so a key
// that is absent and one written as `recall_cooldown: 0` arrive as the same
// value and cannot be told apart; filling both is the only consistent reading.
// A *negative* value is a different matter - nothing fills it in, and validate
// rejects it, because quietly running the 5s default for `recall_cooldown: -5s`
// is precisely the silent no-op this package exists to refuse.
func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.SceneName) == "" {
		c.SceneName = DefaultSceneName
	}
	if c.RecallCooldown == 0 {
		c.RecallCooldown = Duration(5 * time.Second)
	}
	if c.CoalesceWindow == 0 {
		c.CoalesceWindow = Duration(300 * time.Millisecond)
	}
	if c.CoalesceMax == 0 {
		c.CoalesceMax = Duration(3 * time.Second)
	}
	// A cap below the gap it is capping would fire every recall immediately,
	// silently turning the debounce off. Raising it to the gap keeps the
	// old fixed-window behaviour, which is the closest honest reading.
	if c.CoalesceMax >= 0 && c.CoalesceMax < c.CoalesceWindow {
		c.CoalesceMax = c.CoalesceWindow
	}
	// The floor, which is not configurable away, and which doubles as the
	// default. A negative is left alone so validate can name it, rather than
	// have it arrive downstream as a legal-looking ten seconds.
	if c.MinRecallInterval >= 0 && c.MinRecallInterval.Duration() < MinRecallFloor {
		c.MinRecallInterval = Duration(MinRecallFloor)
	}
	// NaN satisfies neither this nor the ceiling in validate, so without the
	// explicit test it survives untouched and the derived limiter interval
	// comes out zero or negative - no rate limiting at all.
	if math.IsNaN(c.Bridge.RequestsPerSecond) || c.Bridge.RequestsPerSecond == 0 {
		c.Bridge.RequestsPerSecond = DefaultRequestsPerSecond
	}
	if c.Bridge.RequestTimeout == 0 {
		c.Bridge.RequestTimeout = Duration(hue.DefaultRequestTimeout)
	}
}

func (c *Config) validate() error {
	// Negatives first, so one is named for what it is instead of being caught
	// by a ceiling or a floor further down under a misleading message. Nothing
	// here means anything below zero, and applyDefaults deliberately leaves
	// negatives intact so they reach this check.
	for _, knob := range []struct {
		name string
		d    time.Duration
	}{
		{"recall_cooldown", c.RecallCooldown.Duration()},
		{"min_recall_interval", c.MinRecallInterval.Duration()},
		{"coalesce_window", c.CoalesceWindow.Duration()},
		{"coalesce_max", c.CoalesceMax.Duration()},
		{"bridge.request_timeout", c.Bridge.RequestTimeout.Duration()},
	} {
		if knob.d < 0 {
			return fmt.Errorf("%s %s is negative; omit the line to get the default", knob.name, knob.d)
		}
	}
	if c.Bridge.RequestsPerSecond < 0 {
		return fmt.Errorf("bridge.requests_per_second %g is negative; omit the line to get the default",
			c.Bridge.RequestsPerSecond)
	}
	if c.CoalesceWindow.Duration() > 5*time.Second {
		return fmt.Errorf("coalesce_window %s is too long to feel responsive", c.CoalesceWindow.Duration())
	}
	if c.CoalesceMax.Duration() > 30*time.Second {
		return fmt.Errorf("coalesce_max %s is too long; a room would sit unrecalled that whole time",
			c.CoalesceMax.Duration())
	}
	// Ceilings, because a bare number means seconds: someone thinking in
	// milliseconds who writes `min_recall_interval: 600` gets ten minutes, and
	// every room silently stops being recalled more than once in that time.
	// Every other tuning knob is range-checked; these two only had a floor.
	if c.MinRecallInterval.Duration() > MaxRecallInterval {
		return fmt.Errorf("min_recall_interval %s is longer than the %s maximum; note a bare number means seconds",
			c.MinRecallInterval.Duration(), MaxRecallInterval)
	}
	if c.RecallCooldown.Duration() > MaxRecallInterval {
		return fmt.Errorf("recall_cooldown %s is longer than the %s maximum; note a bare number means seconds",
			c.RecallCooldown.Duration(), MaxRecallInterval)
	}
	if c.Bridge.RequestsPerSecond > MaxRequestsPerSecond {
		return fmt.Errorf("bridge.requests_per_second %g is above the %d the bridge can be expected to take",
			c.Bridge.RequestsPerSecond, MaxRequestsPerSecond)
	}
	if c.Bridge.RequestTimeout.Duration() < time.Second {
		return fmt.Errorf("bridge.request_timeout %s is too short to complete a request",
			c.Bridge.RequestTimeout.Duration())
	}
	if c.Bridge.Address != "" {
		clean, err := CleanAddress(c.Bridge.Address)
		if err != nil {
			return fmt.Errorf("bridge.address: %w", err)
		}
		c.Bridge.Address = clean
	}
	// Override keys are matched case-insensitively and with surrounding
	// whitespace trimmed, so two keys that differ only in those respects are
	// the same key wearing two hats: one of the two scene ids would win a
	// map-iteration coin flip on every start. There is no reading of that
	// config that is not a mistake, and it needs no bridge to detect, so it
	// is rejected at load time rather than warned about later.
	keys := make([]string, 0, len(c.SmartSceneOverrides))
	for key := range c.SmartSceneOverrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	seen := make(map[string]string, len(keys))
	for _, key := range keys {
		// An empty value is the `Bedroom:` slip - a key written with nothing
		// after the colon. It reads as an override to every later stage, so
		// the group takes the override branch, looks up the scene id "", finds
		// nothing, and is never recalled again. Nothing downstream can tell
		// that apart from a deliberate choice, so it is rejected here.
		if strings.TrimSpace(c.SmartSceneOverrides[key]) == "" {
			return fmt.Errorf("smart_scene_overrides: key %q has no scene; give it a smart scene id or remove the line", key)
		}
		k := normalise(key)
		if prev, dup := seen[k]; dup {
			return fmt.Errorf("smart_scene_overrides: keys %q and %q are the same key once case and surrounding whitespace are ignored", prev, key)
		}
		seen[k] = key
	}
	return nil
}

// CleanAddress validates a bridge address and normalises it to host or
// host:port. Values are spliced straight into a request URL, so a scheme or a
// stray path here would produce baffling errors much later - writing
// "https://..." in a field called "address" is the obvious first mistake.
func CleanAddress(addr string) (string, error) {
	clean := strings.TrimSpace(addr)
	for _, scheme := range []string{"https://", "http://"} {
		if strings.HasPrefix(strings.ToLower(clean), scheme) {
			return "", fmt.Errorf("%q must not include a scheme; use just the host, e.g. 192.168.1.42", addr)
		}
	}
	clean = strings.TrimSuffix(clean, "/")
	if clean == "" {
		return "", fmt.Errorf("%q is empty", addr)
	}
	if strings.ContainsAny(clean, "/?#") {
		return "", fmt.Errorf("%q must be a host or host:port, with no path", addr)
	}
	if host, port, err := net.SplitHostPort(clean); err == nil {
		if host == "" {
			return "", fmt.Errorf("%q has no host", addr)
		}
		// ParseUint at 16 bits, not Atoi: Atoi accepts "-1", "0" and "99999"
		// alike, and all three reach net/url as a URL it rejects with an error
		// naming neither the config key nor the port.
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
			return "", fmt.Errorf("%q has an invalid port; ports run from 1 to 65535", addr)
		}
		if err := checkHost(host); err != nil {
			return "", fmt.Errorf("%q %w", addr, err)
		}
		return clean, nil
	}
	if err := checkHost(clean); err != nil {
		return "", fmt.Errorf("%q %w", addr, err)
	}
	// Deliberately returned as written, bare IPv6 literal and all. Bracketing
	// belongs to hue.hostPort, which does it at every site that builds a URL;
	// doing it here as well would produce "[[::1]]" and, worse, would stop this
	// function accepting its own output - auth persists the cleaned address and
	// resolveAddress cleans it again on every start, so a value the daemon
	// wrote itself has to survive the round trip.
	return clean, nil
}

// checkHost rejects anything that is neither an IP literal nor a plausible
// hostname. Without it `address: "hello world"` is spliced into the URL
// "https://hello world/clip/v2", and the user gets a net/url parse error that
// names no config key at all - the baffling error CleanAddress exists to
// prevent. Underscores are tolerated because some home routers hand them out.
func checkHost(host string) error {
	// An address that has already been through here, or that the user wrote
	// the way a URL wants it, arrives bracketed. Unwrap before validating:
	// net.ParseIP rejects the brackets, and the loop below rejects them again
	// as characters no hostname may contain.
	if len(host) > 1 && host[0] == '[' && host[len(host)-1] == ']' {
		if ip := net.ParseIP(host[1 : len(host)-1]); ip != nil && ip.To4() == nil {
			return nil
		}
		return errors.New("is not a host or host:port: brackets enclose only an IPv6 literal")
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if len(host) > 253 {
		return errors.New("is too long to be a hostname")
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("is not a host or host:port")
		}
		for _, r := range label {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			default:
				return fmt.Errorf("is not a host or host:port: %q cannot appear in a hostname", r)
			}
		}
	}
	return nil
}

// DefaultConfigPath is ~/.config/hue-scene-keeper/config.yaml, honouring
// XDG_CONFIG_HOME.
func DefaultConfigPath() string {
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, "hue-scene-keeper", "config.yaml")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(home, ".config", "hue-scene-keeper", "config.yaml")
}

// DefaultStatePath is ~/.local/state/hue-scene-keeper/credentials.json,
// honouring XDG_STATE_HOME.
func DefaultStatePath() string {
	if dir := os.Getenv("XDG_STATE_HOME"); dir != "" {
		return filepath.Join(dir, "hue-scene-keeper", "credentials.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "credentials.json"
	}
	return filepath.Join(home, ".local", "state", "hue-scene-keeper", "credentials.json")
}

// Lookup is the slice of the registry that exclusion resolution needs.
type Lookup interface {
	Lights() []hue.Light
	Rooms() []hue.Group
	Zones() []hue.Group
	// GroupShadowed reports whether, under the given exclusions, every light
	// in a group resolves to some other group, so the keeper can never select
	// this one. excluded may be nil. See registry.Registry.GroupShadowed.
	GroupShadowed(groupID string, excluded func(string) bool) bool
}

// Exclusions is the resolved form of the config's exclude lists: concrete ids,
// plus the entries that matched nothing so a typo is visible rather than silent.
type Exclusions struct {
	lights map[string]bool
	groups map[string]bool

	// LightNames and GroupNames record what each id resolved from, for logs.
	LightNames map[string]string
	GroupNames map[string]string

	// Unmatched lists config entries that matched no resource.
	Unmatched []string

	// Ineffective lists exclusions that did match a group, but a group the
	// keeper can never select, so the exclusion quietly does nothing. This is
	// deliberately not folded into Unmatched: "matched nothing" is the wrong
	// story for a zone whose name matched perfectly well.
	Ineffective []string

	// OverrideProblems lists smart_scene_overrides keys that match no room or
	// zone, and groups matched by several keys naming different scenes. Both
	// are the same silent-typo failure the Unmatched warnings exist to catch,
	// one struct field further along.
	OverrideProblems []string
}

// ResolveExclusions turns names and ids from the config into resource ids.
// Matching is case-insensitive and accepts either a name or a uuid.
func ResolveExclusions(c *Config, reg Lookup) *Exclusions {
	ex := &Exclusions{
		lights:     map[string]bool{},
		groups:     map[string]bool{},
		LightNames: map[string]string{},
		GroupNames: map[string]string{},
	}

	lights := reg.Lights()
	groups := append(append([]hue.Group{}, reg.Rooms()...), reg.Zones()...)

	for _, want := range c.Exclude.Lights {
		key := normalise(want)
		if key == "" {
			// A blank entry protects nothing, and was the one inert config
			// line this package dropped without a word. Quoted, so an entry
			// that is empty or all spaces is visible in the warning.
			ex.Unmatched = append(ex.Unmatched, "exclude.lights: "+strconv.Quote(want))
			continue
		}
		matched := false
		for _, l := range lights {
			if normalise(l.ID) == key || normalise(l.Name()) == key {
				ex.lights[l.ID] = true
				ex.LightNames[l.ID] = l.Name()
				matched = true
			}
		}
		if !matched {
			ex.Unmatched = append(ex.Unmatched, "exclude.lights: "+want)
		}
	}

	// Two passes: effectiveness depends on the complete exclusion set, since
	// an excluded room cedes its lights to zones and can thereby un-shadow a
	// zone excluded further down the same list.
	type match struct {
		want   string
		groups []hue.Group
	}
	var matches []match
	for _, want := range c.Exclude.Rooms {
		key := normalise(want)
		if key == "" {
			ex.Unmatched = append(ex.Unmatched, "exclude.rooms: "+strconv.Quote(want))
			continue
		}
		var matched []hue.Group
		for _, g := range groups {
			if normalise(g.ID) == key || normalise(g.Name()) == key {
				ex.groups[g.ID] = true
				ex.GroupNames[g.ID] = g.Name()
				matched = append(matched, g)
			}
		}
		if len(matched) == 0 {
			ex.Unmatched = append(ex.Unmatched, "exclude.rooms: "+want)
			continue
		}
		matches = append(matches, match{want: want, groups: matched})
	}
	for _, m := range matches {
		if allShadowed(reg, m.groups, ex.groups) {
			// Typically a zone. Rooms beat zones when the keeper picks the
			// group for a light, so a zone whose lights all sit in live rooms
			// is never selected and excluding it changes nothing. Reporting
			// only exact typos would leave this case - a name the user spelled
			// correctly, that still does nothing - as the one silent failure.
			ex.Ineffective = append(ex.Ineffective, fmt.Sprintf(
				"exclude.rooms: %s matches %s, but every light there is governed by a group that is not excluded - nothing is excluded",
				m.want, describeGroups(m.groups)))
		}
	}

	resolveOverrides(c, groups, ex)

	sort.Strings(ex.Unmatched)
	sort.Strings(ex.Ineffective)
	sort.Strings(ex.OverrideProblems)
	return ex
}

// allShadowed reports whether every group in matched is one no light resolves
// to, i.e. the whole match is inert. A single usable group is enough for the
// exclusion to do something, so a name shared by a room and a zone is not
// reported.
//
// Each group is judged with its own exclusion peeled off: excluding a group
// does something iff some light would resolve to it were it not excluded,
// under the rest of the exclusions. Judging it under its own exclusion would
// find every excluded group shadowed and report the whole list.
func allShadowed(reg Lookup, matched []hue.Group, excluded map[string]bool) bool {
	for _, g := range matched {
		exceptSelf := func(id string) bool { return excluded[id] && id != g.ID }
		if !reg.GroupShadowed(g.ID, exceptSelf) {
			return false
		}
	}
	return true
}

// describeGroups renders groups as `zone "Desk"`, sorted, for a diagnostic.
func describeGroups(groups []hue.Group) string {
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		kind := "group"
		switch g.Type {
		case hue.TypeRoom:
			kind = "room"
		case hue.TypeZone:
			kind = "zone"
		}
		out = append(out, fmt.Sprintf("%s %q", kind, g.Name()))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// resolveOverrides checks smart_scene_overrides against the bridge. Nothing
// else ever does: SmartSceneOverride simply reports no match for a key it does
// not recognise, and the caller falls back to matching by scene name, so a
// misspelled room quietly gets the default treatment it was configured out of.
//
// It also reports a group that several keys claim with different scene ids.
// SmartSceneOverride resolves that deterministically, but a config that has to
// be resolved at all is one the user did not mean to write.
func resolveOverrides(c *Config, groups []hue.Group, ex *Exclusions) {
	if len(c.SmartSceneOverrides) == 0 {
		return
	}
	keys := make([]string, 0, len(c.SmartSceneOverrides))
	for key := range c.SmartSceneOverrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	used := make(map[string]bool, len(keys))
	for _, g := range groups {
		var hits []string
		for _, key := range keys {
			if overrideKeyMatches(key, g) {
				hits = append(hits, key)
				used[key] = true
			}
		}
		if len(hits) < 2 {
			continue
		}
		scenes := map[string]bool{}
		for _, key := range hits {
			scenes[c.SmartSceneOverrides[key]] = true
		}
		if len(scenes) < 2 {
			continue // several keys, one answer: harmless duplication
		}
		winKey, winScene, _ := c.overrideFor(g)
		quoted := make([]string, 0, len(hits))
		for _, key := range hits {
			quoted = append(quoted, strconv.Quote(key))
		}
		ex.OverrideProblems = append(ex.OverrideProblems, fmt.Sprintf(
			"smart_scene_overrides: keys %s all match %s but name different scenes; %q wins, pinning scene %q",
			strings.Join(quoted, ", "), describeGroups([]hue.Group{g}), winKey, winScene))
	}

	for _, key := range keys {
		if !used[key] {
			ex.OverrideProblems = append(ex.OverrideProblems, fmt.Sprintf(
				"smart_scene_overrides: key %q matches no room or zone, so that group keeps the scene picked by name", key))
		}
	}

	// The mirror of the several-keys-one-group case above: one key matching
	// several groups. Two rooms sharing a name both take the same scene id,
	// and the one it does not belong to fails the scene-owns-this-group check
	// at recall time - so that room is silently never recalled.
	for _, key := range keys {
		var matched []hue.Group
		for _, g := range groups {
			if overrideKeyMatches(key, g) {
				matched = append(matched, g)
			}
		}
		if len(matched) < 2 {
			continue
		}
		ex.OverrideProblems = append(ex.OverrideProblems, fmt.Sprintf(
			"smart_scene_overrides: key %q matches %s; a smart scene belongs to one group, so every other one is left unrecalled - use the group id instead",
			key, describeGroups(matched)))
	}
}

// overrideKeyMatches reports whether a config key names a group, by id or name.
// An empty key matches nothing: a group with no name must not be claimed by a
// blank line in the config.
func overrideKeyMatches(key string, group hue.Group) bool {
	k := normalise(key)
	if k == "" {
		return false
	}
	if id := normalise(group.ID); id != "" && k == id {
		return true
	}
	name := normalise(group.Name())
	return name != "" && k == name
}

func normalise(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// LightExcluded reports whether a light is barred from triggering a recall.
func (e *Exclusions) LightExcluded(id string) bool {
	if e == nil {
		return false
	}
	return e.lights[id]
}

// GroupExcluded reports whether a room or zone is never recalled.
func (e *Exclusions) GroupExcluded(id string) bool {
	if e == nil {
		return false
	}
	return e.groups[id]
}

// SmartSceneOverride returns an explicit smart scene id configured for a group,
// matched by the group's id or name.
func (c *Config) SmartSceneOverride(group hue.Group) (string, bool) {
	_, sceneID, ok := c.overrideFor(group)
	return sceneID, ok
}

// overrideFor picks the winning override for a group and names the key it came
// from, which the diagnostic in resolveOverrides needs.
//
// The map is iterated in a random order, so a group named by two keys must not
// be resolved by whichever the runtime happened to visit first: that is a fresh
// coin flip on every process start, and the daemon would recall a different
// scene after a restart with no config change. An id is the more specific way
// to name a group, so an id match wins; anything still tied falls back to the
// normalised key, which at least keeps the choice stable.
func (c *Config) overrideFor(group hue.Group) (key, sceneID string, ok bool) {
	if len(c.SmartSceneOverrides) == 0 {
		return "", "", false
	}
	gid := normalise(group.ID)
	bestByID := false
	bestKey := ""
	for candidate, scene := range c.SmartSceneOverrides {
		if !overrideKeyMatches(candidate, group) {
			continue
		}
		k := normalise(candidate)
		byID := gid != "" && k == gid
		better := !ok ||
			(byID && !bestByID) ||
			(byID == bestByID && k < bestKey)
		if !better {
			continue
		}
		key, sceneID, ok = candidate, scene, true
		bestByID, bestKey = byID, k
	}
	return key, sceneID, ok
}
