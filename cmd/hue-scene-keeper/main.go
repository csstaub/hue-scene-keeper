// Command hue-scene-keeper keeps Hue rooms in their all-day smart scene.
//
// It watches the bridge's event stream. Whenever a light comes on by any
// means, it activates that room's "Natural Light" smart scene, so the bridge
// carries the room through the rest of the day on its own.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	"github.com/csstaub/hue-scene-keeper/internal/logfile"
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
	logFile    string
	logMaxMB   int
	jsonOut    bool
	resetPin   bool

	// configSet records that --config was given explicitly. A missing config
	// file is fine at the default path, where the zero configuration works.
	// At a path the user named, it is a typo. Carrying on with defaults would
	// silently drop every exclusion they think is protecting a room.
	configSet bool

	// logLevelSet records that --log-level was given explicitly, which is what
	// makes it beat the config file's log.level. Without it the two are
	// indistinguishable here: an omitted flag and `--log-level=info` both
	// arrive as "info", and the file could never lower the level at all.
	logLevelSet bool

	// levelVar is the level the handler consults per record, so the config
	// file can move it after the logger already exists. It has to: the logger
	// is built from the flags before any file is read, because most commands
	// never read a config and a broken one must not stop `stop` from stopping
	// the service.
	levelVar *slog.LevelVar
}

// exitStatuser is an error that names the process's exit status itself. Its
// message is never printed: a command returning one has already said whatever
// it had to say, on stdout or through the flag package.
type exitStatuser interface{ ExitStatus() int }

// silent is the plain implementation of exitStatuser.
type silent int

func (s silent) Error() string   { return fmt.Sprintf("exit status %d", int(s)) }
func (s silent) ExitStatus() int { return int(s) }

func main() {
	if err := run(); err != nil {
		var coded exitStatuser
		if errors.As(err, &coded) {
			os.Exit(coded.ExitStatus())
		}
		// Cancellation is deliberately not swallowed here. `run` unwraps it
		// for the daemon, where a signal is how you stop it. For the
		// interactive commands the same error means the thing the user asked
		// for did not happen, and a script running `auth || exit 1` has to
		// hear about that.
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
}

// signalContext cancels the returned context on the first signal and force
// quits on the second.
//
// signal.NotifyContext does the first half, but its goroutine stops listening
// once it has fired while leaving the Notify registration in place. The
// default disposition therefore stays disabled, and a second Ctrl-C does
// nothing at all until the process exits on its own. That is survivable only
// while shutdown is instant. A daemon that ignores the second interrupt is
// one people learn to kill with -9, which is the shutdown a graceful path
// exists to avoid.
func signalContext(signals ...os.Signal) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	// Buffered for both: the second signal may well arrive while the goroutine
	// is between receives, and a dropped one is the bug being fixed here.
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, signals...)
	go func() {
		<-ch
		cancel()
		sig := <-ch
		fmt.Fprintf(os.Stderr, "\n%s again, quitting without finishing the shutdown\n", sig)
		code := 1
		if s, ok := sig.(syscall.Signal); ok {
			// What a shell reports for a process killed by a signal.
			code = 128 + int(s)
		}
		os.Exit(code)
	}()
	return ctx, func() { signal.Stop(ch); cancel() }
}

// bindFlags defines the flag set. Defaults come from def, so the same flags
// can be parsed twice (once before the subcommand and once after) without
// the second pass resetting what the first one set.
func bindFlags(fs *flag.FlagSet, g *globals, def globals) {
	fs.StringVar(&g.configPath, "config", def.configPath, "path to config.yaml")
	fs.StringVar(&g.statePath, "state", def.statePath, "path to credentials.json")
	fs.StringVar(&g.address, "address", def.address, "bridge address (overrides config and discovery)")
	fs.BoolVar(&g.dryRun, "dry-run", def.dryRun, "log intended recalls without sending them")
	fs.StringVar(&g.logFormat, "log-format", def.logFormat, "log format: text or json")
	fs.StringVar(&g.logLevel, "log-level", def.logLevel, "log level: debug, info, warn, error")
	fs.StringVar(&g.logFile, "log-file", def.logFile, "write logs to this file instead of stderr, kept under --log-max-mb")
	fs.IntVar(&g.logMaxMB, "log-max-mb", def.logMaxMB, "size cap for --log-file, in MiB; one previous file is kept beside it")
	fs.BoolVar(&g.jsonOut, "json", def.jsonOut, "machine-readable output (list only)")
	fs.BoolVar(&g.resetPin, "reset-pin", def.resetPin, "auth only: forget the pinned bridge certificate and learn it again")
}

// noteExplicit records which flags were actually typed, for the two where an
// omitted flag and one given at its default value have to be told apart. Both
// flag passes visit it, and both only ever set, so a flag given before the
// subcommand is not forgotten by the pass after it.
func noteExplicit(g *globals) func(*flag.Flag) {
	return func(f *flag.Flag) {
		switch f.Name {
		case "config":
			g.configSet = true
		case "log-level":
			g.logLevelSet = true
		}
	}
}

func usageFor(fs *flag.FlagSet) func() {
	return func() {
		_, _ = fmt.Fprintf(fs.Output(), `%s - keep Hue rooms in their all-day smart scene

usage: %s [flags] <command> [flags]

commands:
  run        watch the event stream and recall scenes (default)
  start      start the installed service (launchd agent or systemd unit)
  stop       stop it, and keep it stopped
  restart    restart it, or start it if it was stopped
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
		logMaxMB:   defaultLogMaxMB,
	}

	var g globals
	fs := flag.NewFlagSet(appName, flag.ContinueOnError)
	bindFlags(fs, &g, defaults)
	fs.Usage = usageFor(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		// ContinueOnError has already printed the message and the usage
		// block. Returning the error would print it a second time, below the
		// usage, where it reads as a second unrelated complaint.
		return silent(1)
	}

	fs.Visit(noteExplicit(&g))

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
			return silent(1)
		}
		sub.Visit(noteExplicit(&g))
		args = sub.Args()
	}

	out, closeLog, err := logOutput(g)
	if err != nil {
		return err
	}
	defer closeLog()

	log, levelVar, err := newLogger(out, g.logFormat, g.logLevel)
	if err != nil {
		return err
	}
	g.levelVar = levelVar
	ctx, stop := signalContext(os.Interrupt, syscall.SIGTERM)
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
		err := cmdRun(ctx, g, log)
		if errors.Is(err, context.Canceled) {
			// Signaling the daemon is how it is stopped, so a cancellation
			// that reached here through a request in flight is a clean exit.
			// Only `run` gets this. For the interactive commands the same
			// error means the work was abandoned halfway.
			return nil
		}
		return err
	case "start":
		return service.Start(ctx, os.Stdout)
	case "stop":
		return service.Stop(ctx, os.Stdout)
	case "restart":
		return service.Restart(ctx, os.Stdout)
	case "status":
		return service.Status(ctx, os.Stdout, service.Paths{Config: g.configPath, State: g.statePath})
	default:
		fs.Usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

// defaultLogMaxMB is the size cap applied to --log-file. One previous file is
// kept beside the live one, so the bytes on disk are bounded at twice this.
const defaultLogMaxMB = 8

// logOutput decides where the logger writes, and returns a function to close
// it with.
//
// Without --log-file that is stderr, which is what a terminal wants, what
// systemd hands to journald, and what the launchd plist redirects. With it,
// the daemon owns the file itself and keeps it under a cap. That is the only
// way to bound a file on macOS: the log lives under a per-user $HOME that a
// root-owned newsyslog rule cannot name, and there is no pid file for it to
// signal a reopen through.
func logOutput(g globals) (io.Writer, func(), error) {
	// Checked even without --log-file, because it is unambiguously a typo
	// wherever it appears.
	if g.logMaxMB < 0 {
		return nil, nil, fmt.Errorf("--log-max-mb %d: must not be negative", g.logMaxMB)
	}
	if g.logFile == "" {
		return os.Stderr, func() {}, nil
	}
	// Zero means the default, as it does everywhere in the config file and
	// for the same reason: there are no pointer flags either, so an omitted
	// one and an explicit --log-max-mb=0 arrive here indistinguishable.
	mb := g.logMaxMB
	if mb == 0 {
		mb = defaultLogMaxMB
	}
	f, err := logfile.Open(g.logFile, int64(mb)<<20)
	if err != nil {
		return nil, nil, fmt.Errorf("--log-file %s: %w", g.logFile, err)
	}
	return f, func() { _ = f.Close() }, nil
}

// newLogger builds the logger, rejecting a level or format it does not know.
//
// Falling back silently means `--log-level=verbose` runs at info and the user
// spends the next hour wondering where their debug output went. Every other
// typo in this CLI is an error; these were the exception.
func newLogger(w io.Writer, format, level string) (*slog.Logger, *slog.LevelVar, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, nil, fmt.Errorf("--log-level %q: want debug, info, warn or error", level)
	}
	// A LevelVar rather than the level itself, so loadAll can hand the config
	// file's log.level to a handler that already exists. The returned var is
	// the only way to move it afterward: the handler holds it as a Leveler
	// and nothing else can reach it.
	levelVar := new(slog.LevelVar)
	levelVar.Set(lvl)
	opts := &slog.HandlerOptions{Level: levelVar}
	var handler slog.Handler
	switch {
	case strings.EqualFold(format, "json"):
		handler = slog.NewJSONHandler(w, opts)
	case strings.EqualFold(format, "text"):
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, nil, fmt.Errorf("--log-format %q: want text or json", format)
	}
	log := slog.New(handler)
	slog.SetDefault(log)
	return log, levelVar, nil
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

// persistPin returns the callback hue.Pin invokes the first time it sees a
// bridge certificate.
//
// It writes the file, because that is the whole of the contract: the Pin
// refuses to trust a certificate it could not persist, so a callback that only
// sets a field in memory satisfies the signature and not the promise. `auth`
// used to be written that way, saving once at the end. A pairing that never
// finished left that process trusting a key nothing had recorded, and the
// next run walked back into the trust-on-first-use window with nothing said. The Pin holds no lock while this runs, so the fsync is its own affair.
func persistPin(creds *config.Credentials, addr string) func(string) error {
	return func(value string) error {
		creds.CertPin = value
		creds.Address = addr
		return creds.Save()
	}
}

// newClient builds a bridge client wired to the persisted key and TLS pin.
// A pin learned on first contact is written back to the credentials file.
func newClient(cfg *config.Config, creds *config.Credentials, addr string, log *slog.Logger) *hue.Client {
	pin := hue.NewPin(creds.CertPin)
	pin.OnLearn = persistPin(creds, addr)
	return hue.New(hue.Options{
		Address:           addr,
		AppKey:            creds.AppKey,
		Pin:               pin,
		RequestsPerSecond: cfg.Bridge.RequestsPerSecond,
		RequestTimeout:    cfg.Bridge.RequestTimeout.Duration(),
		Logger:            log,
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
	applyConfigLogLevel(g, cfg)
	creds, err := config.LoadCredentials(g.statePath)
	if err != nil {
		return nil, nil, err
	}
	return cfg, creds, nil
}

// applyConfigLogLevel moves the live log level to what the config file asks
// for. Here, rather than beside the other flag handling, because this is the
// first point at which a config file has been read at all, and every command
// that reads one comes through loadAll before it logs its first line.
//
// An explicit --log-level wins. A flag typed for this one run is the more
// specific instruction, and it is what a `--log-level=debug` run of a daemon
// whose file says `warn` has to mean. An absent key changes nothing, which is
// what leaves the flag and its info default in charge of every install that
// has not asked for anything else.
func applyConfigLogLevel(g globals, cfg *config.Config) {
	if g.logLevelSet || g.levelVar == nil {
		return
	}
	if lvl, ok := cfg.LogLevel(); ok {
		g.levelVar.Set(lvl)
	}
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

	// Before the link button is pressed, not after. The bridge issues the
	// application key once; discovering then that we cannot store it means the
	// key is lost and the user has to start over.
	if err := creds.EnsureWritable(); err != nil {
		return fmt.Errorf("cannot write credentials to %s: %w", g.statePath, err)
	}

	// Pair with no application key; a successful handshake also learns the pin.
	if g.resetPin {
		fmt.Println("Forgetting the previously pinned bridge certificate.")
		creds.CertPin = ""
	}
	pin := hue.NewPin(creds.CertPin)
	// Written to disk as it is learned, not kept until the pairing succeeds.
	// The handshake that learns it happens on the first attempt, minutes before
	// the user gets around to the link button. Everything after that - giving
	// up, Ctrl-C, closing the laptop - used to leave this process trusting a
	// certificate the next one knew nothing about. Save with no application
	// key is safe here: it refuses to overwrite an existing key with a blank
	// one, and this is a file that either has no key yet or keeps the one it
	// has.
	pin.OnLearn = persistPin(creds, addr)
	client := hue.New(hue.Options{Address: addr, Pin: pin})

	fmt.Printf("Pairing with bridge %s (%s) at %s\n", info.Name, info.ID, addr)
	fmt.Println("Press the round link button on top of the bridge...")

	pairCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	key, err := client.PairWithRetry(pairCtx, appName, 2*time.Second, func(attempt int) {
		if attempt > 1 && attempt%5 == 0 {
			fmt.Println("still waiting for the link button...")
		}
	})
	if err != nil {
		// PairWithRetry reports an interrupt as a timeout waiting for the link
		// button, which is neither what happened nor what the user needs to
		// read after pressing Ctrl-C.
		if ctx.Err() != nil {
			return errors.New("interrupted before the link button was pressed; nothing was paired")
		}
		return err
	}

	creds.AppKey = key
	creds.Address = addr
	creds.BridgeID = info.ID
	if creds.CertPin == "" {
		creds.CertPin = pin.Value()
	}
	if err := creds.Save(); err != nil {
		// The key exists only here now. Printing it is the difference between
		// a fixable problem and pressing the link button again.
		fmt.Fprintf(os.Stderr,
			"\nPaired, but the credentials could not be saved to %s.\nApplication key: %s\nStore it there by hand as {\"app_key\": \"...\"} to avoid re-pairing.\n",
			creds.Path(), key)
		return err
	}
	fmt.Printf("Paired. Credentials written to %s\n", creds.Path())
	return nil
}

// syncTimeout bounds a one-shot read of the bridge's resources. Without it a
// bridge that accepts the connection and then goes quiet leaves a command
// hanging with nothing on screen.
const syncTimeout = 30 * time.Second

// connect loads config and builds a client and an empty registry. It stops
// short of syncing, because resolve syncs through the keeper instead.
func connect(ctx context.Context, g globals, log *slog.Logger) (*config.Config, *hue.Client, *registry.Registry, error) {
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
	return cfg, newClient(cfg, creds, addr, log), registry.New(), nil
}

// prepare connects and syncs the registry, for the read-only commands.
func prepare(ctx context.Context, g globals, log *slog.Logger) (*config.Config, *hue.Client, *registry.Registry, error) {
	cfg, client, reg, err := connect(ctx, g, log)
	if err != nil {
		return nil, nil, nil, err
	}
	syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
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
	// out why their config is not doing what they meant. So an exclusion that
	// matched but can never bite, or an override key that names nothing, has
	// to show up here too, not only in the daemon's log.
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
	// connect, not prepare: Resync does the same full pass of rate-limited
	// GETs that prepare's Sync would, and it is the one that also resolves the
	// exclusion lists Explain reads. Doing both meant two passes, the second
	// of them against the root context, so a bridge that stopped answering
	// mid-pass left resolve hanging with no deadline at all.
	cfg, client, reg, err := connect(ctx, g, log)
	if err != nil {
		return err
	}
	k := keeper.New(client, reg, cfg, log, true)
	syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	if err := k.Resync(syncCtx); err != nil {
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
	client := newClient(cfg, creds, addr, log)
	reg := registry.New()

	if creds.CertPin == "" {
		log.Warn("no pinned bridge certificate yet; the first connection will trust whatever answers")
	}
	// config and state are logged because the two paths are the most common
	// thing to get wrong. `auth` run in an interactive shell picks up
	// XDG_STATE_HOME from the user's rc file; the service manager runs with a
	// bare environment and reads somewhere else. Without these the symptom is
	// an unpaired daemon looping forever, with nothing to show that the two
	// halves are looking at different files.
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
		client = newClient(cfg, creds, addr, log)
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
