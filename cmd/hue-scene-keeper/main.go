// Command hue-scene-keeper keeps Hue rooms in their all-day smart scene.
//
// It watches the bridge's event stream and, whenever a light comes on by any
// means, activates that room's "Natural Light" smart scene so the bridge
// carries the room through the rest of the day on its own.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/config"
	"github.com/csstaub/hue-scene-keeper/internal/hue"
	"github.com/csstaub/hue-scene-keeper/internal/keeper"
	"github.com/csstaub/hue-scene-keeper/internal/registry"
	"github.com/csstaub/hue-scene-keeper/internal/service"
)

const appName = "hue-scene-keeper"

// version is set at build time via -ldflags.
var version = "dev"

type globals struct {
	configPath string
	statePath  string
	address    string
	dryRun     bool
	logFormat  string
	logLevel   string
	jsonOut    bool
	resetPin   bool

	// configSet records that --config was given explicitly. A missing config
	// file is fine at the default path - the zero configuration works - but at
	// a path the user named it is a typo, and carrying on with defaults would
	// silently drop every exclusion they think is protecting a room.
	configSet bool
}

func main() {
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
}

// bindFlags defines the flag set. Defaults come from def, so the same flags can
// be parsed twice - once before the subcommand and once after - without the
// second pass resetting what the first one set.
func bindFlags(fs *flag.FlagSet, g *globals, def globals) {
	fs.StringVar(&g.configPath, "config", def.configPath, "path to config.yaml")
	fs.StringVar(&g.statePath, "state", def.statePath, "path to credentials.json")
	fs.StringVar(&g.address, "address", def.address, "bridge address (overrides config and discovery)")
	fs.BoolVar(&g.dryRun, "dry-run", def.dryRun, "log intended recalls without sending them")
	fs.StringVar(&g.logFormat, "log-format", def.logFormat, "log format: text or json")
	fs.StringVar(&g.logLevel, "log-level", def.logLevel, "log level: debug, info, warn, error")
	fs.BoolVar(&g.jsonOut, "json", def.jsonOut, "machine-readable output (list only)")
	fs.BoolVar(&g.resetPin, "reset-pin", def.resetPin, "auth only: forget the pinned bridge certificate and learn it again")
}

func usageFor(fs *flag.FlagSet) func() {
	return func() {
		_, _ = fmt.Fprintf(fs.Output(), `%s - keep Hue rooms in their all-day smart scene

usage: %s [flags] <command> [flags]

commands:
  run        watch the event stream and recall scenes (default)
  start      start the installed service (launchd agent or systemd unit)
  stop       stop it, and keep it stopped
  status     report whether the service is running
  auth       pair with the bridge by pressing its link button
  discover   list Hue bridges found on the network
  list       show rooms, lights and their smart scenes
  resolve    explain what would happen for one light
  version    print the version

flags:
`, appName, appName)
		fs.PrintDefaults()
	}
}

func run() error {
	defaults := globals{
		configPath: config.DefaultConfigPath(),
		statePath:  config.DefaultStatePath(),
		logFormat:  "text",
		logLevel:   "info",
	}

	var g globals
	fs := flag.NewFlagSet(appName, flag.ContinueOnError)
	bindFlags(fs, &g, defaults)
	fs.Usage = usageFor(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			g.configSet = true
		}
	})

	command := "run"
	args := fs.Args()
	if len(args) > 0 {
		command, args = args[0], args[1:]
	}

	// Accept flags after the subcommand too. `run --dry-run` is what people
	// actually type, and silently ignoring it there would mean touching real
	// lights when the user asked for a dry run.
	if len(args) > 0 {
		sub := flag.NewFlagSet(appName+" "+command, flag.ContinueOnError)
		bindFlags(sub, &g, g)
		sub.Usage = fs.Usage
		if err := sub.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		sub.Visit(func(f *flag.Flag) {
			if f.Name == "config" {
				g.configSet = true
			}
		})
		args = sub.Args()
	}

	log := newLogger(g.logFormat, g.logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Only `resolve` takes positional arguments; anywhere else they are a
	// typo that must not be swallowed.
	if command != "resolve" && len(args) > 0 {
		return fmt.Errorf("unexpected argument %q after %q", args[0], command)
	}

	switch command {
	case "version":
		fmt.Println(appName, version)
		return nil
	case "discover":
		return cmdDiscover(ctx)
	case "auth":
		return cmdAuth(ctx, g, log)
	case "list":
		return cmdList(ctx, g, log)
	case "resolve":
		return cmdResolve(ctx, g, log, args)
	case "run":
		return cmdRun(ctx, g, log)
	case "start":
		return service.Start(ctx, os.Stdout)
	case "stop":
		return service.Stop(ctx, os.Stdout)
	case "status":
		return service.Status(ctx, os.Stdout)
	default:
		fs.Usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func newLogger(format, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	log := slog.New(handler)
	slog.SetDefault(log)
	return log
}

// resolveAddress picks the bridge address from, in order: the flag, the config
// file, the last address we paired with, and finally network discovery.
func resolveAddress(ctx context.Context, g globals, cfg *config.Config, creds *config.Credentials, log *slog.Logger) (string, error) {
	for _, candidate := range []struct{ source, value string }{
		{"--address", g.address},
		{"bridge.address", cfgAddress(cfg)},
		{"credentials", credsAddress(creds)},
	} {
		if candidate.value == "" {
			continue
		}
		clean, err := config.CleanAddress(candidate.value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", candidate.source, err)
		}
		return clean, nil
	}

	log.Info("no bridge address configured, discovering")
	bridges, err := hue.Discover(ctx, 3*time.Second)
	if err != nil {
		return "", err
	}

	// If we have paired before, insist on the same bridge. Picking an
	// arbitrary one would fail the certificate pin later with a confusing TLS
	// error rather than a clear "that is a different bridge".
	if creds != nil && creds.BridgeID != "" {
		for _, b := range bridges {
			if strings.EqualFold(b.ID, creds.BridgeID) {
				log.Info("found paired bridge", "address", b.Address, "id", b.ID, "via", b.Source)
				return b.Address, nil
			}
		}
		return "", fmt.Errorf("paired bridge %s not found on the network (saw %s); "+
			"set bridge.address, or re-pair with `hue-scene-keeper auth`",
			creds.BridgeID, describeBridges(bridges))
	}

	if len(bridges) > 1 {
		log.Warn("several bridges found, using the first",
			"using", bridges[0].Address, "all", describeBridges(bridges))
	}
	log.Info("discovered bridge", "address", bridges[0].Address, "id", bridges[0].ID, "via", bridges[0].Source)
	return bridges[0].Address, nil
}

func cfgAddress(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.Bridge.Address
}

func credsAddress(creds *config.Credentials) string {
	if creds == nil {
		return ""
	}
	return creds.Address
}

func describeBridges(bridges []hue.BridgeInfo) string {
	parts := make([]string, 0, len(bridges))
	for _, b := range bridges {
		parts = append(parts, b.Address+"="+b.ID)
	}
	return strings.Join(parts, ", ")
}

// newClient builds a bridge client wired to the persisted key and TLS pin.
// A pin learned on first contact is written back to the credentials file.
func newClient(cfg *config.Config, creds *config.Credentials, addr string) *hue.Client {
	pin := hue.NewPin(creds.CertPin)
	pin.OnLearn = func(value string) error {
		creds.CertPin = value
		creds.Address = addr
		return creds.Save()
	}
	return hue.New(hue.Options{
		Address:           addr,
		AppKey:            creds.AppKey,
		Pin:               pin,
		RequestsPerSecond: cfg.Bridge.RequestsPerSecond,
		RequestTimeout:    cfg.Bridge.RequestTimeout.Duration(),
	})
}

func loadAll(g globals) (*config.Config, *config.Credentials, error) {
	if g.configSet {
		if _, err := os.Stat(g.configPath); err != nil {
			return nil, nil, fmt.Errorf("--config %s: %w", g.configPath, err)
		}
	}
	cfg, err := config.Load(g.configPath)
	if err != nil {
		return nil, nil, err
	}
	creds, err := config.LoadCredentials(g.statePath)
	if err != nil {
		return nil, nil, err
	}
	return cfg, creds, nil
}

func cmdDiscover(ctx context.Context) error {
	bridges, err := hue.Discover(ctx, 3*time.Second)
	if err != nil {
		return err
	}
	for _, b := range bridges {
		fmt.Printf("%-16s  %s  %s (via %s)\n", b.Address, b.ID, b.Name, b.Source)
	}
	return nil
}

func cmdAuth(ctx context.Context, g globals, log *slog.Logger) error {
	cfg, creds, err := loadAll(g)
	if err != nil {
		return err
	}
	creds.SetPath(g.statePath)

	addr, err := resolveAddress(ctx, g, cfg, creds, log)
	if err != nil {
		return err
	}
	info, err := hue.Probe(ctx, addr)
	if err != nil {
		return fmt.Errorf("cannot reach a bridge at %s: %w", addr, err)
	}

	// Pair with no application key; a successful handshake also learns the pin.
	if g.resetPin {
		fmt.Println("Forgetting the previously pinned bridge certificate.")
		creds.CertPin = ""
	}
	pin := hue.NewPin(creds.CertPin)
	pin.OnLearn = func(value string) error { creds.CertPin = value; return nil }
	client := hue.New(hue.Options{Address: addr, Pin: pin})

	fmt.Printf("Pairing with bridge %s (%s) at %s\n", info.Name, info.ID, addr)
	fmt.Println("Press the round link button on top of the bridge...")

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	key, err := client.PairWithRetry(ctx, appName, 2*time.Second, func(attempt int) {
		if attempt > 1 && attempt%5 == 0 {
			fmt.Println("still waiting for the link button...")
		}
	})
	if err != nil {
		return err
	}

	creds.AppKey = key
	creds.Address = addr
	creds.BridgeID = info.ID
	if creds.CertPin == "" {
		creds.CertPin = pin.Value()
	}
	if err := creds.Save(); err != nil {
		return err
	}
	fmt.Printf("Paired. Credentials written to %s\n", creds.Path())
	return nil
}

// prepare loads config, connects, and syncs the registry. Shared by the
// read-only commands and by run.
func prepare(ctx context.Context, g globals, log *slog.Logger) (*config.Config, *hue.Client, *registry.Registry, error) {
	cfg, creds, err := loadAll(g)
	if err != nil {
		return nil, nil, nil, err
	}
	creds.SetPath(g.statePath)
	if creds.AppKey == "" {
		return nil, nil, nil, fmt.Errorf("%w (looked in %s)", config.ErrNoCredentials, g.statePath)
	}
	addr, err := resolveAddress(ctx, g, cfg, creds, log)
	if err != nil {
		return nil, nil, nil, err
	}
	client := newClient(cfg, creds, addr)

	reg := registry.New()
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := reg.Sync(syncCtx, client); err != nil {
		return nil, nil, nil, err
	}
	return cfg, client, reg, nil
}

type listGroup struct {
	Name       string      `json:"name"`
	ID         string      `json:"id"`
	Type       string      `json:"type"`
	Excluded   bool        `json:"excluded"`
	SmartScene *listScene  `json:"smart_scene"`
	Lights     []listLight `json:"lights"`
}

type listScene struct {
	Name  string `json:"name"`
	ID    string `json:"id"`
	State string `json:"state"`
}

type listLight struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	On       bool   `json:"on"`
	Excluded bool   `json:"excluded"`
}

func cmdList(ctx context.Context, g globals, log *slog.Logger) error {
	cfg, _, reg, err := prepare(ctx, g, log)
	if err != nil {
		return err
	}
	excl := config.ResolveExclusions(cfg, reg)

	groups := append(append([]hue.Group{}, reg.Rooms()...), reg.Zones()...)
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Type != groups[j].Type {
			return groups[i].Type < groups[j].Type
		}
		return groups[i].Name() < groups[j].Name()
	})

	out := make([]listGroup, 0, len(groups))
	for _, grp := range groups {
		entry := listGroup{
			Name: grp.Name(), ID: grp.ID, Type: grp.Type,
			Excluded: excl.GroupExcluded(grp.ID),
		}
		if scene, ok := reg.SmartSceneForGroup(grp.ID, cfg.SceneName); ok {
			entry.SmartScene = &listScene{Name: scene.Name(), ID: scene.ID, State: scene.State}
		}
		for _, id := range reg.LightIDsInGroup(grp.ID) {
			light, ok := reg.Light(id)
			if !ok {
				continue
			}
			on, _ := reg.LightIsOn(id)
			entry.Lights = append(entry.Lights, listLight{
				Name: light.Name(), ID: id, On: on, Excluded: excl.LightExcluded(id),
			})
		}
		out = append(out, entry)
	}

	if g.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	for _, grp := range out {
		marker := ""
		if grp.Excluded {
			marker = "  [EXCLUDED - never recalled]"
		}
		fmt.Printf("%s %s%s\n", strings.ToUpper(grp.Type), grp.Name, marker)
		if grp.SmartScene == nil {
			fmt.Printf("  scene: NONE - no smart scene named %q; create it in the Hue app\n", cfg.SceneName)
		} else {
			fmt.Printf("  scene: %s (%s)\n", grp.SmartScene.Name, orUnknown(grp.SmartScene.State))
		}
		for _, l := range grp.Lights {
			state := "off"
			if l.On {
				state = "on"
			}
			note := ""
			if l.Excluded {
				note = "  [excluded - never triggers]"
			}
			fmt.Printf("    %-28s %-3s %s%s\n", l.Name, state, l.ID, note)
		}
		fmt.Println()
	}
	for _, entry := range excl.Unmatched {
		fmt.Printf("warning: %s matched nothing on this bridge\n", entry)
	}
	// The other two diagnostic classes. `list` is where a user looks to find
	// out why their config is not doing what they meant, so an exclusion that
	// matched but can never bite, or an override key that names nothing, has to
	// show up here too rather than only in the daemon's log.
	for _, entry := range excl.Ineffective {
		fmt.Printf("warning: %s\n", entry)
	}
	for _, entry := range excl.OverrideProblems {
		fmt.Printf("warning: %s\n", entry)
	}
	return nil
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func cmdResolve(ctx context.Context, g globals, log *slog.Logger, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: hue-scene-keeper resolve <light name or id>")
	}
	cfg, client, reg, err := prepare(ctx, g, log)
	if err != nil {
		return err
	}
	k := keeper.New(client, reg, cfg, log, true)
	if err := k.Resync(ctx); err != nil {
		return err
	}
	fmt.Print(k.Explain(strings.Join(args, " ")))
	return nil
}

func cmdRun(ctx context.Context, g globals, log *slog.Logger) error {
	cfg, creds, err := loadAll(g)
	if err != nil {
		return err
	}
	creds.SetPath(g.statePath)
	if creds.AppKey == "" {
		return fmt.Errorf("%w (looked in %s)", config.ErrNoCredentials, g.statePath)
	}
	addr, err := resolveAddress(ctx, g, cfg, creds, log)
	if err != nil {
		return err
	}
	client := newClient(cfg, creds, addr)
	reg := registry.New()

	if creds.CertPin == "" {
		log.Warn("no pinned bridge certificate yet; the first connection will trust whatever answers")
	}
	// config and state are logged because the two paths are the most common
	// thing to get wrong: `auth` run in an interactive shell picks up
	// XDG_STATE_HOME from the user's rc file, while the service manager runs
	// with a bare environment and reads somewhere else. Without these the
	// symptom is an unpaired daemon looping forever with nothing to show that
	// the two halves are looking at different files.
	log.Info("starting",
		"version", version,
		"bridge", addr,
		"config", g.configPath,
		"state", g.statePath,
		"scene", cfg.SceneName,
		"dry_run", g.dryRun,
		"recall_cooldown", cfg.RecallCooldown.Duration(),
		"min_recall_interval", cfg.MinRecallInterval.Duration())

	// An address the user pinned by hand is never overridden by discovery:
	// they know where the bridge is, and quietly wandering off to a different
	// one would be worse than saying it is unreachable.
	pinnedByHand := g.address != "" || cfgAddress(cfg) != ""

	for {
		k := keeper.New(client, reg, cfg, log, g.dryRun)
		err = k.Run(ctx)
		switch {
		case errors.Is(err, context.Canceled):
			log.Info("shutting down")
			return nil
		case !errors.Is(err, hue.ErrStreamUnreachable):
			// Either a clean stop or something retrying cannot fix - a revoked
			// application key, a certificate that no longer matches the pin.
			// Fail loudly so the service manager reports it.
			return err
		}

		// The bridge stopped answering where we were looking. Most often its
		// DHCP lease moved, which no amount of retrying the old address fixes
		// - and the address is read back from the credentials file, so a
		// restart would not fix it either.
		log.Warn("bridge unreachable", "bridge", addr, "err", err)
		if !pinnedByHand {
			if found, ok := rediscoverAddress(ctx, creds, log); ok && found != addr {
				addr = found
				creds.Address = found
				if err := creds.Save(); err != nil {
					log.Warn("could not persist the new bridge address", "err", err)
				}
			}
		}

		if err := sleepCtx(ctx, bridgeRetryPause); err != nil {
			log.Info("shutting down")
			return nil
		}
		// A fresh registry: whatever we cached is now of unknown age, and
		// onConnect's reconnect diff would read it as the state we last saw.
		client = newClient(cfg, creds, addr)
		reg = registry.New()
		log.Info("reconnecting to bridge", "bridge", addr)
	}
}

// bridgeRetryPause spaces out supervisor cycles. The stream has already spent
// its own backoff budget before giving up, so this only stops a bridge that is
// simply switched off from turning into a discovery loop.
const bridgeRetryPause = 30 * time.Second

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// rediscoverAddress looks for the bridge we are paired with, by id. It reports
// false if discovery found nothing matching, in which case the caller keeps
// the address it has - a bridge that is merely rebooting will answer there
// again shortly.
func rediscoverAddress(ctx context.Context, creds *config.Credentials, log *slog.Logger) (string, bool) {
	if creds == nil || creds.BridgeID == "" {
		return "", false
	}
	bridges, err := hue.Discover(ctx, 3*time.Second)
	if err != nil {
		log.Warn("rediscovery failed", "err", err)
		return "", false
	}
	for _, b := range bridges {
		if strings.EqualFold(b.ID, creds.BridgeID) {
			log.Info("found the paired bridge at a new address",
				"address", b.Address, "id", b.ID, "via", b.Source)
			return b.Address, true
		}
	}
	log.Warn("paired bridge not found by discovery",
		"id", creds.BridgeID, "saw", describeBridges(bridges))
	return "", false
}
