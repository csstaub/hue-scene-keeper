# hue-scene-keeper

A small daemon that keeps your Philips Hue rooms in their **Natural Light** smart scene.

Whenever a light comes on — from the app, a wall switch, a motion sensor, or mains power
returning after a cut — the daemon activates that room's Natural Light smart scene. Because
the scene is left *active*, the bridge itself then carries the room through the rest of the
day's colour-temperature transitions with no further involvement from the daemon.

Single static Go binary, no runtime dependencies. Runs on macOS, Linux, and a Raspberry Pi.

## Why

Hue's own per-lamp power-on behaviour can pin a fixed brightness and colour, but it cannot
follow a schedule. A lamp that comes back at 23:00 gets the same cold daylight value as one
that comes back at 09:00. This daemon closes that gap by handing the job to the smart scene
the Hue app already builds for you.

## Install

On macOS, build an installer and run it. This puts the binary in
`/usr/local/bin`, installs a launchd agent, and starts it:

```sh
git clone <this repo> && cd hue-scene-keeper
go tool mage pkg:build    # dist/hue-scene-keeper-<version>.pkg
open dist/*.pkg           # or: go tool mage pkg:install
```

The `.pkg` is unsigned, so Gatekeeper will refuse a double-click: right-click it
and choose Open, or install it from the command line with `go tool mage
pkg:install`. `go tool mage pkg:uninstall` removes everything it installed and leaves
your config and credentials alone.

Or just build the binary, on any platform:

```sh
go tool mage go:build     # or: go tool mage go:dist   for all platforms
go tool mage go:install   # sudo-copies it to /usr/local/bin/hue-scene-keeper
```

Run `go:install` as yourself, not under `sudo`. It compiles as you and asks for
`sudo` only for the copy into `/usr/local/bin`; running the whole target as root
would run the compiler as root too, and the root-owned entries it leaves in your
build and module caches break every later build you make as yourself.

## Setup

```sh
# 1. Pair with the bridge. Press its round link button when prompted.
hue-scene-keeper auth

# 2. Check every room has a Natural Light smart scene.
hue-scene-keeper list

# 3. Watch what it would do, without touching a light.
#    Stop the installed service first, if you have one: a dry run only
#    promises that *this* process touches nothing.
hue-scene-keeper status
hue-scene-keeper stop
hue-scene-keeper run --dry-run

# 4. Run it for real.
hue-scene-keeper run
```

`auth` writes an application key and the bridge's pinned TLS key to
`~/.local/state/hue-scene-keeper/credentials.json` (mode 0600, written via a
temporary file and rename).

If you ever replace or factory-reset the bridge, its certificate changes and the
pin will refuse to connect; re-pair with `hue-scene-keeper auth --reset-pin`.

If `list` reports `scene: NONE` for a room, that room has no Natural Light smart scene —
create one in the Hue app. The daemon will not invent a substitute.

## Configuration

Optional. Copy [`config.example.yaml`](config.example.yaml) to
`~/.config/hue-scene-keeper/config.yaml`. The defaults work without a file.

```yaml
scene_name: "Natural Light"     # localised by the Hue app; `list` shows yours

exclude:
  rooms:  ["Bedroom"]           # never recalled at all
  lights: ["Night Stand Left"]  # never *trigger* a recall
```

### What "excluded" means

The two lists do different things, and the difference matters:

| | Effect |
|---|---|
| `exclude.rooms` | The room or zone is **never recalled**, and an excluded room **cedes its lights** to any zone that holds them. |
| `exclude.lights` | The light **never triggers** a recall. It is still turned on by a recall another light in its room caused. |

So excluding a night stand lamp means switching it on at 2am won't light up the whole
bedroom. It does *not* mean the lamp stays off when you switch on the ceiling light — that
recall lights the whole room, night stand included. If you want a room left entirely alone,
put it in `exclude.rooms` — and make sure no zone overlaps it, because ceded lights fall
to their zones (exclude those zones too if you have them).

### Carving a light out of a room

The ceding rule is how you manage *part* of a room. Say the bedroom has a ceiling light
that should recall Natural Light, and a night light that should only ever be touched by
hand:

1. In the Hue app, make a zone holding just the ceiling light and give it a
   Natural Light scene.
2. Exclude the room: `exclude.rooms: ["Bedroom"]`.

The room's exclusion hands the ceiling light to the zone, so switching it on recalls the
*zone's* scene — which doesn't include the night light. The night light belongs to no
group at all now: it never triggers anything and no recall ever touches it.
`resolve <light>` shows exactly this reasoning per light.

## Commands

| Command | Purpose |
|---|---|
| `run` | The daemon. This is the default. |
| `start` / `stop` / `restart` | Start, stop, or restart the installed service through launchd or systemd. |
| `status` | Whether that service is running, and whether it comes back on its own. |
| `auth` | Pair with the bridge via its link button. |
| `discover` | List bridges found on the network. |
| `list` | Rooms, zones, lights, and the smart scene each maps to. `--json` for raw output. |
| `resolve <light>` | Explain exactly what would happen for one light, and why. |

Useful flags: `--dry-run`, `--address`, `--config`, `--log-format=json`, `--log-level=debug`,
`--log-file`.
Flags work either side of the subcommand — `run --dry-run` and `--dry-run run` are
equivalent — and an unrecognised flag or stray argument is an error rather than
being silently ignored.

## Running as a service

**Linux (systemd)**. Pair *before* starting the service, as the service user and
with the same `--state` path the unit uses — the unit passes its paths explicitly
so that these two cannot drift apart:

```sh
sudo useradd --system --home-dir /var/lib/hue-scene-keeper --shell /usr/sbin/nologin hue
sudo install -d -o hue -g hue -m0700 /var/lib/hue-scene-keeper
sudo install -d -m0755 /etc/hue-scene-keeper
sudo install -m0644 deploy/hue-scene-keeper.service /etc/systemd/system/

sudo -u hue /usr/local/bin/hue-scene-keeper \
    --state /var/lib/hue-scene-keeper/credentials.json auth

sudo systemctl daemon-reload
sudo systemctl enable --now hue-scene-keeper
journalctl -u hue-scene-keeper -f
```

**macOS (launchd)** — `go tool mage pkg:build` does all of this for you; do it by hand
only if you would rather not install a package:

```sh
cp deploy/dev.staub.hue-scene-keeper.plist ~/Library/LaunchAgents/
launchctl enable gui/$(id -u)/dev.staub.hue-scene-keeper
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/dev.staub.hue-scene-keeper.plist
tail -F ~/Library/Logs/hue-scene-keeper.log
```

The `enable` line matters and it has to come first. `hue-scene-keeper stop`
writes the label into launchd's per-user disabled database to keep the agent
stopped across logins, and that database outlives the plist — deleting the file
does not clear it. Bootstrapping a label that is still on it fails with
`Bootstrap failed: 5: Input/output error`, which says nothing about why.
`launchctl print-disabled gui/$(id -u)` lists what is on it.

The plist needs no editing. launchd does not expand variables in
`StandardOutPath`, and an agent's working directory is `/` rather than your home
directory, so the daemon is started through a shell that expands `$HOME` in your
own session. That is what lets one file in `/Library/LaunchAgents` give every
user their own config, credentials, and log.

It runs as **you**, at login — not as root at boot. That is deliberate: on macOS
15 and later, reaching the local network needs the user's consent, and a process
inside your GUI session can ask for it. A `LaunchDaemon` has no session to ask
in, and would be denied silently, which is exactly the traffic this daemon is
made of.

Pairing is the one thing the installer cannot do for you. Until you run
`hue-scene-keeper auth`, `run` exits immediately and launchd retries it every 30
seconds; once the credentials exist it picks them up on its own, with nothing to
restart.

**That self-recovery is launchd's, not systemd's.** systemd rate-limits
restarts — ten in five minutes — so a unit enabled before pairing gives up in
under a minute with `start-request-repeated-too-quickly` and stays failed. That
limit is deliberate: without it an unrecoverable error would restart-loop into
the journal forever. It does mean that if you enabled the unit first and paired
afterwards, you have to clear the counter by hand:

```sh
sudo systemctl reset-failed hue-scene-keeper
sudo systemctl start hue-scene-keeper
```

Pairing before `systemctl enable --now`, as above, avoids this entirely.

### Logs

On Linux there is nothing to configure: the unit logs to the journal, and
`journalctl` already bounds what it keeps.

On macOS the agent writes to `~/Library/Logs/hue-scene-keeper.log`, and the
daemon keeps that file under a cap itself — `--log-max-mb`, 8 MiB by default.
At the cap it renames the file to `.log.1` and starts a new one, so there is
always between one and two caps' worth of history and never more. That is a
size limit rather than a rotation: exactly one old file is kept, and nothing
older survives. `--log-max-mb=64` if you would rather keep more.

Use `tail -F`, not `tail -f`. `-f` follows the file's inode, so once the log is
renamed aside it keeps showing you the archive and appears to go quiet.

The same works anywhere else you run the daemon by hand:

```sh
hue-scene-keeper run --log-file /var/log/hue-scene-keeper.log --log-max-mb 32
```

Without `--log-file` the daemon logs to stderr and nothing is capped, which is
what a terminal, systemd, and a shell redirect of your own all want.

### Starting and stopping it

```sh
hue-scene-keeper status   # is anything running in the background?
hue-scene-keeper stop     # stop it, and keep it stopped
hue-scene-keeper start    # start it, and have it come back on its own again
hue-scene-keeper restart  # bounce it, e.g. after editing the config
```

`status` answers the question worth asking before a dry run:

```
service:   launchd agent dev.staub.hue-scene-keeper
plist:     /Library/LaunchAgents/dev.staub.hue-scene-keeper.plist
state:     running (pid 94644)
at login:  starts on its own

A background daemon is recalling scenes right now.
Stop it with `hue-scene-keeper stop` before running with --dry-run.
```

Both drive the platform's own service manager — `launchctl` on macOS,
`systemctl` on Linux — and echo every command they run, so nothing happens that
you could not have typed yourself. There is no `--dry-run` equivalent here and
no pid file: the manager is the one that knows whether the daemon should be
running, and it is the only thing that can make the answer stick.

`stop` is persistent by design. launchd would otherwise reload the agent at your
next login and systemd at the next boot, and a daemon that quietly returns while
you are testing with `--dry-run` looks exactly like a bug in `--dry-run`. So
`stop` disables the job as well as halting it, and `start` re-enables it.

On Linux the system unit needs root; `start` and `stop` go through `sudo` for you
when you are not already root.

## How it works

```
light turns on  ──▶  light → owner device → room
                     room → smart_scene named "Natural Light"
                     PUT /clip/v2/resource/smart_scene/<id>
                         {"recall": {"action": "activate"}}
```

The daemon holds one long-lived SSE connection to `/eventstream/clip/v2` and acts on two
signals:

- **A light goes off → on.** The everyday case.
- **A device's Zigbee connectivity returns.** This is the one an off→on edge cannot see:
  when a lamp's mains is cut while the bridge stays up, the cached state still says "on",
  and the lamp is on again when it returns — so there is no edge to detect. Connectivity is
  the only reliable signal for a power cut.

Connectivity has a floor worth knowing about. A mains cut only triggers a recall if it
lasted long enough for the bridge to mark the lamp unreachable, which takes about a
minute; a shorter cut produces no event at all. Cut a lamp's power for ten seconds and
restore it, and the bridge still reports the lamp as on and connected the whole way
through — the daemon is told nothing and correctly does nothing. Polling would not close
the gap either: under that floor the bridge's own picture of the lamp never changes, so
there is nothing to poll for. A cut of two or three minutes recalls the scene as
expected.

### Not chasing its own tail

Recalling a room turns on *every* light in it, and the bridge reports each one as newly on.
Left alone, each of those echoes is a fresh off→on trigger and the daemon recalls forever.

Three things prevent that:

1. A recall waits for the room to go quiet for `coalesce_window` (300ms) and is
   **deduplicated by room**, so a burst is one recall.
2. After recalling a room, triggers from it are ignored for `recall_cooldown` (5s).
3. A hard floor of 10s between recalls of the same room, which **cannot be configured
   lower** — it is the last line of defence.

The test suite includes a fake bridge that responds to a recall by emitting on-events for
every light in the room, asserting that exactly one recall results. It is the most important
test in the repo; disabling the suppression makes it fail immediately.

### Sharing the house with another automation

`coalesce_window` is an idle gap, not a fixed window: any further activity on a room's
lights restarts the wait, up to `coalesce_max` (3s). Flipping one switch sees no further
activity and is acted on one gap later, so that stays as responsive as it sounds.

This matters when something else is writing to the same lights — a HomeKit or GPS
automation turning the house on when you arrive, say. Those commands land over several
seconds. Without the gap the daemon would style a room on the first light to come on, and
the automation's remaining commands would land on top of the scene it just applied. Those
late commands arrive at lights the scene has *already turned on*, so they carry no off→on
edge, and nothing would ever trigger a correction: the room would sit half-styled until
someone touched a switch. Waiting for quiet means the daemon writes last.

It is not a guarantee. If the other automation leaves gaps longer than `coalesce_window`
between two lights in the same room, the daemon can still act in the middle of it. Raise
`coalesce_window` if you see that, at the cost of some responsiveness on a manual switch.

If the bridge refuses a recall for a transient reason — 429, 503, a timeout — it is retried
three times with a growing delay. A refusal it cannot recover from, such as a deleted scene,
is logged once and not repeated.

### Security

The bridge serves a self-signed certificate, so ordinary chain verification cannot work.
The daemon pins the SHA-256 of the bridge's SubjectPublicKeyInfo on first contact and fails
closed on any later mismatch.

## Development

Builds are driven by [mage](https://magefile.org), pinned in `go.mod` as a tool
dependency — there is nothing to install first.

```sh
go tool mage -l          # list every target
go tool mage go:test     # go test ./...
go tool mage go:race     # go test -race ./...
go tool mage go:vet
go tool mage go:lint     # golangci-lint
go tool mage pkg:build   # macOS installer, unsigned
```

Targets are grouped into two namespaces: `go:` for anything driven by the Go
toolchain, `pkg:` for the macOS installer. `all` and `clean` sit at the top level
because they span both. They live in [`magefiles/`](magefiles/magefile.go), and
`go tool mage` with no target runs `all`: vet, then test, then build.

golangci-lint is kept in a separate `lint.mod` rather than the module's own
`go.mod`, so its large dependency tree and higher Go floor do not become
requirements for building the daemon.

Tests need no hardware: `internal/hue/fake` implements enough of a bridge — resource
endpoints, smart scene recall, and a scriptable SSE stream — to run the daemon end to end.

## Licence

Copyright © 2026 Cedric Staub

Licensed under the EUPL, either version 1.2 or — as soon as they are approved by
the European Commission — later versions of the EUPL. The full text is in
[`LICENSE`](LICENSE), and official translations into the other EU languages are
at <https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12>.
