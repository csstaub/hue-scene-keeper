# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All tooling goes through mage, pinned as a Go tool dependency — nothing to install first.

| Command | What it does |
| --- | --- |
| `go tool mage` | Default target: vet, test, build |
| `go tool mage go:lint` | `go tool -modfile=lint.mod golangci-lint run` |
| `go tool mage go:race` | `go test -race ./...` |
| `go tool mage go:fmt` | `gofmt -l -w .` |
| `go tool mage go:dist` | Cross-build all five release targets into `dist/` |
| `go tool mage -l` | List every target |

Run all four of `go tool mage`, `go tool mage go:lint`, `go tool mage go:race`, and `go tool mage go:fmt` before considering a change done.

Targets under a namespace need their prefix. Mage's fuzzy matching will accept `lint` for `go:lint`, but the namespaced form is the real name and the one to write.

The `-modfile=lint.mod` is load-bearing: golangci-lint's dependency tree and its higher Go floor live in `lint.mod`/`lint.sum` so the daemon's `go.mod` stays tiny and keeps its lower floor. Plain `golangci-lint run` is not the project's way.

Single test: `go test ./internal/keeper -run TestName -v`. Mage has no single-test target.

`TestManualDiscovery` hits the real network and is skipped unless `HUE_MANUAL=1` (optionally `HUE_ADDRESS=<ip>`).

## Style

- Prefer the standard library. The daemon depends only on `gopkg.in/yaml.v3`; a well-justified new dependency is fine, but reach for stdlib first.
- Tests use stdlib `testing` and `httptest` only — no testify or other assertion libraries.
- Commit directly to `main`; no branch or PR workflow.

## Invariants

These encode real failure modes. Changing them needs deliberate thought, not a refactor.

- **The 10s recall floor is not configurable.** `config.MinRecallFloor` silently raises any lower `min_recall_interval`. It is the last defence against a recall → light-on event → recall feedback loop. `TestDoesNotLoopWhenRecallTurnsOnWholeRoom` (driven by `fake.Bridge.EchoOnRecall`) is the regression test for exactly this; if suppression logic changes, it must still pass.
- **Suppression is armed before the recall request is sent**, not after — the bridge starts turning lights on the moment it accepts the recall. It is rolled back on *any* failure, including a timeout where the bridge may in fact have applied the recall. Keeping it armed there looks safer but strips the room of its recovery: the recall floor that `rollback` also restores is what blocks that recall's own retry. The echo a timed-out recall produces is covered by `recall_cooldown`, which is shorter than the request timeout that got us there.
- **Activity extends a pending recall but never creates one.** `kindActivity` is emitted for every light event that is *not* an off→on edge, and `schedule` only ever uses it to push out a deadline for a group already in `pending`. That asymmetry is the whole reason a recall's own echo cannot feed itself: by the time the bridge reports the lights the recall turned on, `drain` has already removed the entry that caused them, so there is nothing to extend and activity cannot conjure a new entry. If you ever let activity create an entry, `TestDoesNotLoopWhenRecallTurnsOnWholeRoom` is what will tell you.
- **`coalesce_window` is an idle gap, not a fixed window.** It is what stops the daemon writing a scene into the middle of another automation's whole-house burst. The late half of such a burst arrives at lights the scene has already turned on, so it carries no off→on edge and would never trigger a correcting recall — the room would just stay half-styled. `coalesce_max` caps the deferral so a chattering light cannot starve its room. `TestActivityInAPendingRoomDefersTheRecall` is the regression test.
- **The debounce runs on `schedNow()` (real time), not the `k.now` test hook.** Its deadlines are waited out by a real `time.Timer`, so the two clocks must agree. `k.now` stays the *policy* clock — the recall floor and the suppression window — which is the part tests move by hand. Putting the debounce on `k.now` strands every pending entry the moment a test freezes it.
- **Recalls are sent from the `sender` goroutine, but every decision stays on dispatch.** `commit` runs the floor check and arms suppression, the sender only performs the HTTP request, and `finish` handles the answer — so `suppressUntil`/`lastRecall`/`inFlight` are still single-goroutine state with no locking. The split exists so a slow bridge delays one room instead of freezing the scheduling of every room behind it. `inFlight` is what stops a second recall for a group stacking behind one already on the wire.
- **`exclude.rooms` and `exclude.lights` mean different things.** An excluded *room* is never recalled at all. An excluded *light* never triggers a recall, but is still lit when another light in its room causes one. Do not collapse these.
- **There are two trigger sources, not one.** Off→on edges miss the mains-power case: when a lamp loses power while the bridge stays up, the cached state still reads "on", so no edge occurs. Zigbee connectivity returning to `connected` is the only signal for that, and it has a floor. The exemption a `power_restored` trigger carries from `commit`'s "is any light still on" veto is sticky on the pending entry (`foldedPowerRestore`), because the lookup deliberately never writes the returning lamp back into the registry — reading it off whichever reason created the entry meant a restore folded into an ordinary switch-on was silently vetoed and dropped. A mains cut only triggers a recall if it lasted long enough for the bridge to mark the lamp unreachable, which takes about a minute; a shorter cut produces no event at all. Measured on a real bridge: a ~10s cut leaves `on: true` and `status: connected` unchanged throughout, so both triggers are blind to it; a 2-3 minute cut works end to end. Polling would not close the gap — under that floor the bridge's own model of the lamp never moves, so there is nothing to poll for. This is an accepted limitation; say so plainly in user-facing text rather than promising that a power cycle always triggers a recall.
- **`hue.Client` deliberately has no `http.Client.Timeout`** — it would cap the long-lived event stream. Per-request deadlines come from context: `do()` derives one from `Options.RequestTimeout` (10s default), and `Stream` deliberately does not go through `do()`. The deadline starts **after** `lim.wait` returns, not at entry — starting it at entry would spend most of the budget queueing and time out requests that were never sent.
- **The stream watchdog is armed before `c.http.Do`, not after it returns.** Nothing else bounds the wait for response headers — no `Timeout`, no `ResponseHeaderTimeout` — so a bridge that completes TLS and then never answers would block in `Do` forever with the watchdog not yet running. `TestStreamWatchdogArmedBeforeRequest` is the regression test.
- **The transport sets `ForceAttemptHTTP2: false` deliberately.** The watchdog recovers a wedged bridge by cancelling the request context, which under HTTP/2 only sends `RST_STREAM` and returns the TCP connection to the pool — so the reconnect opens a fresh stream on the same dead pipe and recovers nothing. Under HTTP/1.1 cancelling closes the connection, which is what the watchdog means. `TestStreamWatchdogOpensANewConnection` counts accepted connections and fails if h2 is re-enabled. Note `httptest.NewTLSServer` is HTTP/1.1 only, so a test must set `EnableHTTP2` explicitly to exercise this at all.
- **`Stream` returns rather than retrying forever on errors retrying cannot fix.** A revoked application key (403) or an `ErrPinMismatch` ends `Run`, so the service manager sees a failure instead of a process that is alive, logging retry warnings, and doing nothing. `Retryable` returns false for `ErrPinMismatch` for the same reason. A watchdog teardown counts as a *failure* even though the connection was open the whole time — otherwise the uptime check resets the backoff and a mute bridge is retried at full speed forever.
- **After `MaxConsecutiveFailures` the stream gives up with `ErrStreamUnreachable` and `cmdRun` rediscovers the bridge by id.** A bridge whose DHCP lease moved is never coming back at the old address, and that address is re-read from the credentials file on every restart, so retrying it was unrecoverable without hand-editing JSON. An address set with `--address` or `bridge.address` is never overridden by discovery.
- **TLS pinning is trust-on-first-use** over the bridge's SubjectPublicKeyInfo and fails closed on mismatch. `hue.Options.Insecure` disables it and is test-use-only.

## Gotchas

- Pairing must run as the user that runs the service. Credentials default to `$XDG_STATE_HOME/hue-scene-keeper/credentials.json` (mode 0600, written via CreateTemp+rename with both file and dir synced). The systemd unit passes `--config`/`--state` explicitly rather than relying on XDG, because `auth` run as the service user would otherwise write to that user's home and the service would read a different file.
- The config file is entirely optional — a missing file is not an error.
- `magefiles/` carries a `//go:build mage` tag, which is why `.golangci.yml` needs no exclusion for it.
- The launchd plist in `deploy/` needs no templating: it runs the daemon under `/bin/sh -c` so `$HOME` expands in the user's own session, which is the only way a system-wide plist in `/Library/LaunchAgents` can reach a per-user log path.
- In the pkg postinstall, `launchctl enable` must run **before** `launchctl bootstrap`. A label in launchd's per-user disabled database makes bootstrap fail with `Bootstrap failed: 5: Input/output error`, and that database outlives the plist, so enabling afterwards never recovers. Check with `launchctl print-disabled gui/$(id -u)`.
- A light switched on during the initial ~1.5s startup resync is missed, and this is accepted. The opening GET already reports it as on, so no off→on edge ever occurs, and `apply_on_startup` is off by default. Reconnects do not have this hole — `onConnect` diffs lit lights before and after the resync — only the very first connect does, once per daemon lifetime, over a window the user would have to hit by hand. Closing it would mean recalling on state the daemon never saw change.
