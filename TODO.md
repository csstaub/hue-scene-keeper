# TODO — bug review findings

Review date: 2026-09-05, against commit `b35d377`.

Scope: all ~8k lines of Go (`cmd/`, `internal/{keeper,hue,config,registry,service}`, `magefiles/`) plus
`deploy/` and `build/`. Five independent reviews were run over separate areas of concern: the keeper
state machine, the hue client/eventstream, config+registry+credentials, the CLI/service/packaging
layer, and a cross-cutting concurrency pass.

Baseline tooling was clean before and after: `go vet ./...`, `go tool mage go:lint` (0 issues), and
`go test -race -count=1 ./...` all pass. Every finding below is something the tooling does not catch.
Note that `.golangci.yml` enables only `default: standard`, so `bodyclose`, `contextcheck`, and
`errorlint` are not running.

Findings were checked against the deliberate invariants in `CLAUDE.md`; designs documented there are
not reported as bugs. Where a finding touches an invariant, it is because the code does not deliver
what the invariant promises. Several findings were confirmed empirically with throwaway tests; those
are marked **(verified)**.

**Counts:** 2 High · 18 Medium · 25 Low.

---

## Status

Fixed in `4d395a7`, `eb28f81`, `62f5b25`, `709cc5a`, `7ed8086`. All four gates
(`go tool mage`, `go:lint`, `go:race`, `go:fmt`) pass after each.

| Fixed | |
| --- | --- |
| H1, H2, M2, M3, M10 | the stream-deafness cluster, plus regression tests that fail without each fix |
| M1 | power-restore exemption made sticky across a fold |
| M6, M7, M9 | override and config-path validation |
| M8, L14 | recall-timer ceilings, duration overflow, NaN rate limit |
| M11, L20, L21 | pairing preflight, key rescue, corrupt-file repair path, fail-closed overwrite guard |
| M12 | resolved config/state paths logged |
| M15, M16, M17, L39 | fake bridge fidelity |
| M18 | certificate pinning now has tests |
| L1, L27 | dead backoff entry, strict log flags |
| L8, L9 | watchdog vs resync stamping, ticker floor |

**M5 closed as working-as-intended, not fixed.** Implementing the proposed split
(keep suppression armed on `ErrRequestTimeout`) broke
`TestWedgedRecallTimesOutRatherThanHangingForever`: the recall floor that
`rollback` also restores is what unblocks that recall's *own retry*, so keeping
it armed costs the room its recovery as well as its recall. `CLAUDE.md` already
said this ("including blocking its own retry"); the reviewer who argued it was
not a real problem was right. The unconditional rollback now carries a comment
explaining why, so the next reader does not re-open it.

**Deliberately not fixed** (see "What I'd skip" reasoning): L2, L11, L15, L22,
L23, L24, L25, L26, L28–L38, and the M14/L3–L7/L10/L12/L13/L16–L19 remainder.
Most need topologies this deployment does not have, or are packaging polish that
only matters for distribution. L25 (no second-signal force quit) becomes worth
doing if M13's blocking shutdown drain is ever added; M13 itself is still open.

---

## High

### H1. The event stream can hang forever waiting for response headers **(verified)**
`internal/hue/eventstream.go:150` (the `c.http.Do(req)` call) vs. `eventstream.go:162-179` (watchdog).

The staleness watchdog is started *after* `c.http.Do` returns. `c.http` deliberately has no
`Timeout`, the transport sets no `ResponseHeaderTimeout`, and `Stream` deliberately bypasses `do()` —
so nothing bounds the wait for response headers.

A bridge that accepts the TCP connection, completes TLS, reads the request, and then never sends a
status line (wedged/overloaded bridge, or a middlebox) blocks `Do` indefinitely. `ReadTimeout` never
applies because the watchdog goroutine does not exist yet; `TLSHandshakeTimeout` and `Dialer.Timeout`
are both already satisfied. The daemon goes permanently deaf — no log line, no reconnect, until the
process is restarted. This is exactly the failure the watchdog exists to prevent.

Verified: with a TLS server whose handler never calls `WriteHeader`, `Stream` was still stuck after
10s with `ReadTimeout: 300ms`.

Fix: move the watchdog goroutine and the initial `lastRead.Store` above the `c.http.Do` call —
`ctx`/`cancel` already exist at that point, so cancelling aborts the in-flight `Do`.
(`Transport.ResponseHeaderTimeout` is an HTTP/1.1-only alternative; the h2 transport ignores it — see H2.)

### H2. Under HTTP/2 the watchdog's forced reconnect reuses the same TCP connection **(verified)**
`internal/hue/client.go:258` (`ForceAttemptHTTP2: true`) with `internal/hue/eventstream.go:173-174` (`cancel()`).

Cancelling the request context under h2 sends `RST_STREAM` for that one stream and leaves the TCP
connection in the transport pool. The next `streamOnce` opens a *new h2 stream on the same
connection*. If the connection itself is the problem — the only scenario the watchdog exists for —
the reconnect is a no-op and the daemon loops forever against the same dead pipe. No
`ReadIdleTimeout`/PING is configured on the h2 transport, and `New` does not retain the
`*http.Transport`, so nothing can call `CloseIdleConnections()`.

Verified against a wedged silent server with `ReadTimeout: 150ms`:
- `EnableHTTP2 = true`: **1 TCP connection accepted, 27 HTTP requests served**
- HTTP/1.1: **13 TCP connections, 13 requests** — 1:1, as intended

`TestStreamWatchdogReconnectsOnSilence` (`eventstream_test.go:292`) uses `httptest.NewTLSServer`,
which negotiates **HTTP/1.1 only**. The suite therefore exercises the path that works while
`ForceAttemptHTTP2: true` ships the one that does not — this hides H2 completely.

Fix (pick one): configure `http2.Transport.ReadIdleTimeout`/`PingTimeout` via
`http2.ConfigureTransports`; or give `Stream` its own `*http.Transport` the watchdog can
`CloseIdleConnections()` on; or drop `ForceAttemptHTTP2` for the stream. Either way add a variant of
the watchdog test with `srv.EnableHTTP2 = true` and a connection-counting listener.

---

## Medium

### M1. A `power_restored` trigger folded into a pending entry loses its veto exemption
`internal/keeper/keeper.go:608-613`, with `keeper.go:910`.

`schedule` folds a second trigger for an already-pending group into `extend(p)` and **never updates
`p.recall`**. The entry keeps the reason, scene, and light name of whichever trigger created it.
`commit` then reads `r := p.recall` and applies the "no light is on any more" veto on that stale reason:

```go
if r.reason != reasonPowerRestored && !k.reg.GroupHasLightOn(r.groupID) {
```

The comment at `keeper.go:901-909` explains why `power_restored` must be exempt: `runLookup`
deliberately never writes the lamp it read back into the registry (`keeper.go:776-778`), so the
registry has no record of the lamp returning and `GroupHasLightOn` would veto every restore.

Sequence:
1. Lamp A in Kitchen is `on:false` in the registry (turned off in the app), then its mains is cut and
   the bridge marks it unreachable.
2. Lamp B in Kitchen flicks on and straight back off → pending entry for `room-kitchen` with
   `reason = light_switched_on`; the off event is `kindActivity` and extends it.
3. Lamp A's mains returns → connectivity event → lookup → `GetLight` reports `on:true` →
   `emit(kindLight, reason=power_restored)`.
4. `schedule` sees the existing entry → `extend` only. Reason stays `light_switched_on`.
5. `drain` → `commit`: reason is not `power_restored`, `GroupHasLightOn` is false (A's `on` was never
   written back, B is off) → **the restore is vetoed and dropped.** No retry; the room stays unstyled.

The reverse is also wrong but benign: an entry created by `power_restored` keeps the exemption for
every later trigger folded into it, skipping a veto that should apply.

Fix: promote the exemption when folding — if `r.reason == reasonPowerRestored && p.reason !=
reasonPowerRestored`, set `p.reason = reasonPowerRestored` (and refresh `lightName` for the log). Or
make it a sticky `exemptFromOnCheck` bool on `pendingRecall`, OR-ed on every fold.

### M2. Watchdog-triggered reconnects never engage backoff **(verified)**
`internal/hue/eventstream.go:102-106`.

`if time.Since(start) >= healthyConnection { failures = 0 }`. A connection that is open but silent
still counts as healthy once it has lasted 60s — which a watchdog trip (≥ `ReadTimeout`, default 15
min) always has. `failures` resets to 0 on every trip, so the retry loop never escalates: connect →
15-20 min of silence → immediate reconnect, forever, at ~3-4 cycles/hour with only a `Warn`. Combined
with H2 this is an unbounded, non-recovering loop; the 4s h2 experiment produced 27 reconnects with
zero backoff.

Fix: have `streamOnce` return a sentinel `errStreamSilent` for a watchdog teardown and skip the
`failures = 0` reset for it, so a persistently wedged bridge backs off and the log escalates.

### M3. `Stream` has no terminal failure — a permanent auth or pin failure spins forever
`internal/hue/eventstream.go:62-107` (reached from `keeper.go:229`), with `internal/hue/client.go:76-91`
and `client.go:142-145`.

`Stream` returns only `ctx.Err()`; every other outcome is a retryable failure with backoff capped at
10 minutes. A revoked app key (403 on `/eventstream`) or `ErrPinMismatch` after a bridge replacement
produces an endless `WARN event stream disconnected, retrying`. The process stays alive, so
launchd/systemd never observe a failure and the user sees a "running" service that does nothing.
`onConnect` failures have the same shape.

`ErrPinMismatch` compounds this: it has no consumer anywhere in the repo (grep confirms only its
definition and a doc mention), and `Retryable` returns `true` for it. The one condition the pinning
exists to detect — possibly a MITM — is retried forever, and the carefully worded remediation text at
`client.go:143-144` ("re-pair with `--reset-pin`") is buried in a retry warning instead of surfaced
as fatal. In `finish` it also burns three recall retries.

Fix: make `Retryable` return `false` for `errors.Is(err, ErrPinMismatch)`; classify the error from
`streamOnce` and return immediately on `ErrPinMismatch` and on 401/403, so `Run` returns a real error,
logs at `Error`, and the supervisor reports it.

### M4. Envelope-level errors are always classified retryable, bypassing `StatusError.Retryable`
`internal/hue/client.go:416-418` (also `:377`, `:393`).

CLIP v2 reports many failures as HTTP 200 with a populated `errors` array. `RecallSmartScene` turns
those into a plain `fmt.Errorf`, which `Retryable` (`client.go:90`) falls through to `return true`. A
permanent refusal ("smart scene not found", "group unavailable") delivered in the envelope is retried
through the full `recallBackoff` — precisely what `StatusError.Retryable` was written to prevent; the
doc comment at `client.go:47-52` claims that distinction is made.

Fix: give envelope errors their own type carrying the bridge's description, and classify the "not
found"/validation shapes as non-retryable — or default them to non-retryable, matching the
"needs a human" reasoning in the comment.

### M5. Rollback on a *timed-out* recall disarms the loop defence — **reviewers disagreed**
`internal/keeper/keeper.go:998`, with `internal/hue/client.go:351` and `client.go:68`.

Two reviewers reached this independently and drew opposite conclusions. Recorded unresolved; it needs
a human decision.

The invariant says suppression is "rolled back if the bridge *refuses* the recall: no echo is
coming." But `finish` calls `k.rollback(p)` unconditionally on `res.err != nil`, and
`ErrRequestTimeout` is by construction the ambiguous case — the PUT reached the bridge and only the
*response* was late.

- **Argues it is a real Medium:** T0 `commit` arms `suppressUntil[G]=T0+5s`, `lastRecall[G]=T0`;
  bridge accepts and lights the room at T0+9s; at T0+10s the per-request deadline fires; `finish`
  rolls back, deleting **both** the suppression window and the min-recall floor for G. The on-events
  from T0+9s are in `k.triggers` while the outcome is in `k.results`, and `select` picks uniformly —
  if the results case wins, those triggers resolve with no suppression *and* no floor, producing a
  fresh recall (the queued retry is then discarded as superseded). It converges after one extra
  recall, but the room is visibly re-styled and the floor — the last line of defence — was removed by
  our own code.
- **Argues it is not:** `RecallCooldown` (5s) expires well before the 10s request timeout, so the
  echo is already gone by the time rollback runs, and the retry limit bounds it. Could not construct
  a real loop.

Both agree on the shape; they disagree on whether the window is reachable. If it is judged real, the
fix is to split rollback by error class — roll back only for `StatusError` (a genuine refusal) and
pre-flight failures; on `ErrRequestTimeout` leave `suppressUntil`/`lastRecall` armed and let the
floor gate the retry.

### M6. `smart_scene_overrides` values are never validated — an empty value silently kills a room's recall forever **(verified)**
`internal/config/config.go:232-244` (validate) and `:445-489` (resolveOverrides), consumed at
`internal/keeper/keeper.go:859-866`.

`validate()` checks override *keys* for normalisation collisions and `resolveOverrides()` checks keys
against the bridge, but nothing ever inspects the *value*. `smart_scene_overrides:\n  Bedroom:` — a
very common YAML slip — decodes to `map[string]string{"Bedroom": ""}`. `SmartSceneOverride` returns
`("", true)`, so `smartSceneFor` takes the override branch, `reg.SmartScene("")` misses, and the room
is never recalled. The only signal is a per-recall log line, not a load-time or resync-time
diagnostic. This is exactly the silent-typo class `resolveOverrides` was written to eliminate.

Compounded by M17 (registry inserts empty-ID resources, making `SmartScene("")` return `ok=true` for
a zero-value id).

Fix: reject empty/whitespace-only values in `validate()` alongside the duplicate-key check, and treat
an override id that resolves to no scene as an `OverrideProblems` entry in `resolveOverrides`.

### M7. One override key matching several same-named groups produces no diagnostic **(verified)**
`internal/config/config.go:456-464`.

`resolveOverrides` only reports when *one group* is claimed by *two or more keys*
(`if len(hits) < 2 { continue }`). The mirror case — one key matching two groups that share a name —
is never reported. With two rooms both named "Bedroom" and `smart_scene_overrides: {Bedroom:
scene-x}`, both take `scene-x`; the one it doesn't belong to fails `scene.Group.RID != group.ID` in
`smartSceneFor` and is silently never recalled. Confirmed: `OverrideProblems=[]`, both groups resolve
to `scene-x`.

Fix: also accumulate key→matched-group counts in the group loop and emit an `OverrideProblems` entry
when one key matches more than one group.

### M8. No upper bound on `min_recall_interval`/`recall_cooldown`, so a unit slip silently disables the daemon **(verified)**
`internal/config/config.go:192-194`, `:203-218`.

Every other knob is range-checked (`coalesce_window` ≤ 5s, `coalesce_max` ≤ 30s, `request_timeout` ≥
1s, `requests_per_second` ≤ 20). The two recall timers are checked only against a *lower* floor.
Because a bare number means seconds (`config.go:48-54`), a user thinking in milliseconds who writes
`min_recall_interval: 600` gets **10 minutes**, accepted silently — every room is recalled at most
once per 10 minutes and the daemon looks broken. Confirmed: `min_recall_interval: 600` →
`minrecall=10m0s`, no error.

Worse, overflow is platform-dependent: `min_recall_interval: 1e300` saturates to `2562047h47m16s` on
darwin/arm64 but wraps to the *negative* minimum int64 on linux/amd64 (a release target), where the
floor clamp then quietly turns it into 10s. Same input, two different meanings across shipped builds.

Fix: add sanity ceilings (e.g. ≤ 1h for both) to `validate()`, and range-check the float in
`Duration.UnmarshalYAML` before the `time.Duration` conversion so overflow is an error.

### M9. An explicit `--config` path that does not exist is silently ignored **(verified)**
`cmd/hue-scene-keeper/main.go:274` → `internal/config/config.go:147-151`.

"Missing config is not an error" is right for the *default* path but is applied to an explicitly
passed one too. Verified: `hsk --config /definitely/not/here.yaml run` never mentions the path and
proceeds with an all-defaults config. An admin who installs `config.yaml` but names it `config.yml`,
or a unit file (`deploy/hue-scene-keeper.service:37`) that outlives a path change, gets a daemon
running with **no exclusions at all** — `exclude.rooms` protection vanishes silently and the daemon
recalls rooms the user deliberately fenced off.

Fix: record whether `-config` was set (`fs.Visit`, or compare against `defaults.configPath`) and make
`os.IsNotExist` fatal when it was.

### M10. A paired bridge that changes IP can never be re-found; discovery is dead code after first pairing
`cmd/hue-scene-keeper/main.go:188-231` (credentials candidate at `:192`), `internal/hue/eventstream.go:73-107`.

`creds.Address` is written by `cmdAuth` (`:337`) and again by `newClient`'s `OnLearn` (`:260-261`), so
after any successful pairing the credentials candidate always wins and `hue.Discover` is never
reached again — including the "paired bridge %s not found on the network" branch at `:220`, which is
unreachable for any paired install.

When DHCP hands the bridge a new lease after a router reboot, `Stream` retries the dead address
forever, backing off to the 10-minute cap, logging only `event stream disconnected, retrying` with no
mention of the address. Restarting the daemon does not help — the stale address is re-read from the
credentials file. Only hand-editing `credentials.json`, setting `bridge.address`, or `--address`
recovers it.

Fix: after N consecutive stream failures, re-run `Discover` and accept the address of the bridge whose
ID matches `creds.BridgeID` (the matching logic already exists at `:213-223`), and persist it. At
minimum, log the address and a "re-pair or set bridge.address" hint on repeated failure.

### M11. `auth` can lose a freshly issued application key
`cmd/hue-scene-keeper/main.go:342-344`.

Writability of the state path is never checked before the user is asked to press the link button. If
`creds.Save()` fails — wrong user (the exact case `deploy/hue-scene-keeper.service:14` warns about), a
`--state` under an unwritable directory, a read-only `/var/lib` — the key the bridge just issued
exists nowhere: it is not printed and the error message does not contain it. The user must press the
button again, and every failed attempt leaves an orphan whitelist entry on the bridge.

Fix: preflight the directory (`CreateTemp`+remove) before `PairWithRetry`, and print the key on a
`Save` failure so it can be recovered by hand.

### M12. The daemon never logs which config/state paths it resolved
`cmd/hue-scene-keeper/main.go:522-528`.

The `starting` line carries version, bridge, scene, and timings but not `configPath`/`statePath`,
which makes the two realistic path mismatches undiagnosable. Concrete macOS failure: the user has
`XDG_STATE_HOME` exported from `~/.zshrc`, so `hue-scene-keeper auth` in Terminal writes there, while
the launchd agent (`deploy/dev.staub.hue-scene-keeper.plist:39`, non-interactive `/bin/sh -c`, sources
no rc file) reads `~/.local/state/...`. Result: the documented unpaired restart loop — `no credentials
found` every 30s, forever, with nothing in the log showing the two are looking at different files.
Same shape on Linux for a `--user` unit.

Fix: add `"config", g.configPath, "state", g.statePath` to the startup log, and print the resolved
state path in `ErrNoCredentials`'s message.

### M13. A pending or in-flight recall is dropped on SIGTERM and cannot be recovered after restart
`internal/keeper/keeper.go:210-235`, `cmd/hue-scene-keeper/main.go:531-535`.

Cancelling ctx tears down dispatch (`keeper.go:554`) and cancels the sender's HTTP request mid-flight;
entries sitting in `pending` for the coalesce window are discarded without a word. After the
supervisor restarts, `onConnect` takes the `first` branch (`keeper.go:262-269`) and does no diffing,
so the light that is already on produces no off→on edge and the room is never styled — it stays
half-lit until someone toggles it. The window is real: `systemctl restart` or a pkg upgrade landing in
the seconds after someone walks into a room.

Fix: on shutdown, drain `pending`/`inFlight` under a bounded `context.WithoutCancel` +
`WithTimeout` (2-3s); at minimum log `"dropping N pending recalls"` so the behaviour is visible.
Note this interacts with L25 (no second-signal force quit) — adding a blocking drain makes that gap bite.

### M14. `runLookup` keeps issuing GETs after the context is cancelled
`internal/keeper/keeper.go:780-792`.

The loop `continue`s on error without ever checking `ctx.Err()`, so on shutdown a worker walks the
whole `lightIDs` list issuing doomed requests. Not a hang (each returns instantly from `lim.wait`),
but during a *live* bridge outage each light burns a rate-limiter slot and up to a full 10s request
timeout, and there are only four workers for a whole-house restore.

Fix: `if ctx.Err() != nil { return }` at the top of the loop body.

### M15. The fake sends SSE keepalives every 200ms; the real bridge is silent for minutes
`internal/hue/fake/fake.go:544`, `:558-562`.

The `StreamOptions.ReadTimeout` doc (`eventstream.go:23-32`) records from a real capture that the
bridge sends `": hi"` at connect and then *nothing* for minutes. The fake does the opposite: a
`: keepalive\n\n` every 200ms, and never sends the opening `": hi"`. Every integration test therefore
runs against a stream that is never idle, so `stampReader`, the watchdog, and anything depending on
tolerating multi-minute silence are untested end to end. A regression that made the client require
inbound traffic to make progress would pass the whole suite and go deaf in the field.

Fix: make the keepalive opt-in (`Bridge.Keepalive(d)`), default off, and send a single `": hi"` at
connect to match the capture.

### M16. The fake never validates `hue-application-key`
`internal/hue/fake/fake.go:370-384`, `:524-542`.

No handler inspects the header. A real bridge returns 403 `unauthorized user` for a missing or
revoked key. A regression that dropped `req.Header.Set("hue-application-key", ...)`
(`client.go:320-322`, `eventstream.go:144-146`) would pass every test and fail against every real
bridge.

Fix: reject requests whose key is not `"test-key"` with 403 and a CLIP-shaped error body.

### M17. The fake answers unknown resource types with 200 + empty data instead of 404
`internal/hue/fake/fake.go:481-507` — `collect` has no `default:` case, so an unrecognised rtype falls
through to `writeEnvelope(w, nil)`, emitting `{"errors":[],"data":[]}`.

A typo'd or renamed rtype (`"lights"`, `"smart_scenes"`) would make `GetResource` return an empty
slice with `err == nil`; the daemon would conclude the home has no lights and sit idle. A real bridge
returns 404.

### M18. `auth.go`, `Pin.verify`, and `parseARecords` have zero test coverage
`internal/hue/auth.go` (whole file), `client.go:116-147`, `discover.go:335-376`.

- The fake registers only `/eventstream/clip/v2` and `/clip/v2/resource/` (`fake.go:71-72`); there is
  no `/api` handler, so `Pair`/`PairWithRetry` and the entire link-button flow are never executed.
- Every test constructs clients with `Insecure: true` (`eventstream_test.go:37`, `client_test.go:137,166`,
  `fake.go:91`), which sets `VerifyPeerCertificate = nil` (`client.go:252-254`). The security-critical
  TOFU path — empty chain, parse failure, learn, `OnLearn` failure, mismatch — is never exercised.
- `parseARecords` parses attacker-reachable UDP and is touched only by the `HUE_MANUAL` network test.

Fix: table tests for `Pin.verify` (no network needed — call it directly with `[][]byte` from a
generated self-signed cert), a `/api` handler on the fake, and byte-level tests for
`parseARecords`/`skipName`.

---

## Low

### Keeper

**L1. `recallBackoff`'s last element is unreachable — 3 delays, only 2 ever used.**
`internal/keeper/keeper.go:58`, `:1008`, `:1022`. Trace: `attempt=0` → fail → uses `[0]=1s`;
`attempt=1` → fail → uses `[1]=3s`; `attempt=2` → `attempts=3 >= 3` → give up. Total sends 3, delays
used `{1s, 3s}` — **`9 * time.Second` is dead data.** `TestRetriesAreBoundedAndReleaseTheRoom` asserts
`Attempts() == len(recallBackoff)`, pinning current behaviour. The trap is for the next editor: adding
a fourth delay yields 4 attempts using `[0..2]`, so the new value is again never read. Fix: drop the
trailing `9s`, or change the guard to `attempts > len(recallBackoff)` and adjust the test.

**L2. Suppression is keyed on the recalled group, but the echo is attributed per-light — a zone recall is not suppressed.**
`keeper.go:921` (arm) vs `keeper.go:831` (check). For a *room* recall these agree. For a *zone* recall
they do not: the bridge lights every light in the zone, and `GroupForLight` prefers the room for any
that has one (`registry.go:382-387`). Those off→on edges resolve to their *rooms*, are not covered by
the zone's suppression window, and each schedules its own recall — one zone recall fans out into a
recall of every room in that zone. One hop, not a loop (room recalls are self-suppressed and floored),
and it needs a mixed zone (at least one roomless light plus room-owning lights), which `registry.go:412-420`
notes is unusual. Fix: at commit time arm suppression for the group *and* every distinct
`GroupForLight(l)` over `LightIDsInGroup(r.groupID)`; rollback must undo the same set.

**L3. `coalesce_max` stops capping for a group whose recall is in flight.**
`keeper.go:667-668`. `hardAt` is documented as "fixed when the entry is created" (`keeper.go:116-117`)
and is the only thing stopping a chattering light deferring its room forever, but the in-flight
deferral pushes it out. Each push is bounded by one round trip, so a chattering light plus a slow
bridge extends deferral past `coalesce_max`. Raising `hardAt` here *is* necessary (else `extend`
becomes a permanent no-op), so this is cap-correctness, not an outright break. Fix: push `fireAt` to
`later(p.hardAt, schedNow().Add(window))` without moving `hardAt` — or accept and document it.

**L4. An excluded light's activity still defers its room's recall.**
`keeper.go:589-602` with `keeper.go:416`. `emitActivity` has no exclusion check, and `schedule`'s
`kindActivity` branch calls `extend` without consulting `excl.LightExcluded`. A light excluded
*because* it is noisy (TV backlight, lamp on a dynamic scene) still pushes out every recall in its
room, up to `coalesce_max`. Does not violate the documented rooms/lights split literally, but gives an
excluded light ongoing influence over its room's timing. Worth a deliberate decision either way —
one reading is that such activity *should* count as "the room is still being written to."

**L5. No `recover()` on any spawned goroutine.** `keeper.go:216-222`. No currently-reachable panic was
found (`recallBackoff[p.attempt]` is provably in range; `Name()` nil-checks `Metadata`; `Exclusions`
has nil-receiver guards at `config.go:510`/`:518`; `bridges[0]` at `main.go:227` is guarded). Risk is
forward-looking: dispatch decodes and indexes bridge-supplied data with no net.

**L6. `keeper.New` nil-checks `log` but not `cfg`.** `keeper.go:182-206`. A nil `cfg` panics later on
the dispatch goroutine (`k.cfg.CoalesceWindow` at `:616`) rather than at construction.

### Hue client / stream

**L7. Unbounded buffering in the SSE reader.** `eventstream.go:189-203`, `:208-233`. (Found
independently by two reviewers.) `br.ReadString('\n')` has no size cap — `bufio.NewReaderSize` sets
only the *initial* buffer — and `data strings.Builder` accumulates until a blank line. A bridge that
streams bytes without a newline, or `data:` lines without a terminating blank line, grows the heap
without bound. The peer is TLS-pinned, so this is a robustness/OOM concern rather than an attack
surface, but the deliberate move away from `bufio.Scanner` (documented at `:187-188`) swapped silent
truncation for unbounded allocation. Fix: cap the accumulated frame (8-16MB, well above the 300KB the
oversize test uses); drop the frame and error out past the cap so the connection recycles.

**L8. The stream watchdog counts the resync as silence.** `eventstream.go:162-185`. `lastRead` is
stamped at connect, then `onConnect(ctx)` (→ `Keeper.Resync`, six rate-limited GETs) runs *before* the
read loop starts, with nothing stamping `lastRead` in between. A resync longer than `ReadTimeout`
would have the watchdog cancel its own connection, failing the resync and looping. Harmless at the
15-minute default (the keeper never sets `ReadTimeout`), but the two mechanisms are coupled in a way
that is easy to break. Fix: re-stamp `lastRead` after `onConnect` returns, or start the watchdog after it.

**L9. `time.NewTicker(readTimeout / 3)` panics for a sub-3ns `ReadTimeout`.** `eventstream.go:165`;
only `readTimeout <= 0` is defaulted (`:67-70`). Unreachable from config today, but `ReadTimeout` is
an exported option and an unrecovered panic here kills the daemon. Fix: `max(readTimeout/3, time.Second)`.

**L10. IPv6 addresses are concatenated into URLs without brackets.** `discover.go:222` with
`discover.go:166` and `client.go:294`. `discoverCloud` validates with `net.ParseIP` (which accepts
IPv6) then appends the bare literal, while the port branch just above at `:219` correctly uses
`net.JoinHostPort` — the two paths disagree. Same concatenation in `Client.url`, so
`bridge.address` set to an IPv6 literal yields `https://fe80::1/clip/v2/...` and a confusing "invalid
port" error. Fix: normalise once via `net.JoinHostPort` / `url.URL`.

**L11. mDNS: no response validation, ctx cancellation ignored, read errors swallowed.**
`discover.go:258-271`, `:335-376`. `parseARecords` harvests A records from *every* section of *any*
arriving datagram without checking the QR bit, transaction ID, that the answer relates to
`_hue._tcp.local` (`dnsTypeSRV`/`dnsTypePTR` are declared but only `dnsTypeA` is matched at `:370`),
or that the source is on a local subnet (`ReadFromUDP`'s source is discarded at `:261`). The comment
at `:228-229` says over-collecting is safe because `Probe` filters, but `Probe` only confirms the peer
*looks like* a bridge (`:182-184`), not that it is *your* bridge — so a LAN host answering
`/api/config` with a `bridgeid` becomes an adoption candidate and can capture the TOFU pin and app
key. Inherent to unauthenticated mDNS, and the `discover` CLI prints id/name for the user to verify,
so this is a hardening/doc note — but the code is less selective than its comment implies. Separately
the read loop honours only `ctx.Deadline()` (`:252-256`), not cancellation, so Ctrl-C waits out the
full timeout, and a genuine socket error is indistinguishable from the deadline (`:262-263`), yielding
`mdnsErr == nil` and a misleading "no Hue bridge found".

**L12. `PairWithRetry` aborts on transient errors.** `auth.go:77-79`. Only `ErrLinkButton` continues
the loop; a 503 or connection reset during the 2-minute pairing window — exactly when the user is
poking at the bridge — aborts pairing and forces a re-run plus another button press. Fix: also
continue when `Retryable(err)` and the deadline has not passed.

**L13. `Pin.OnLearn` is invoked while holding `p.mu`, and the auth path does not actually persist.**
`client.go:127-139`; callers at `main.go:259-263` and `:318`. `OnLearn` runs under a non-reentrant
mutex, so any future implementation reading `pin.Value()` deadlocks the TLS handshake goroutine — and
it performs fsync'd file I/O inside a `VerifyPeerCertificate` callback. Separately, `cmdAuth`'s
`OnLearn` (`main.go:318`) only assigns an in-memory field, which does not satisfy the contract the
comment at `client.go:130-133` spells out; it is safe only because `cmdAuth` saves on the success
path. Fix: snapshot the callback and release the mutex before invoking it; document that `OnLearn`
must not call back into the `Pin`.

### Config / registry / credentials

**L14. `requests_per_second: .nan` passes both range checks and disables rate limiting entirely.**
**(verified)** `config.go:195-197`, `:211-214`. NaN satisfies neither `<= 0` nor `> MaxRequestsPerSecond`,
so it survives untouched. `newLimiter` (`client.go:156-161`) computes
`time.Duration(float64(time.Second) / NaN)` = `0` on arm64, `MinInt64` on amd64 — either way `wait()`
never delays and the budget protecting the bridge is gone. (`.inf` is correctly rejected.) Fix:
`math.IsNaN` check in `applyDefaults`.

**L15. Explicit `0` and negative values are silently replaced by defaults instead of rejected.**
`config.go:177-200`. Every guard is `<= 0`, so `recall_cooldown: 0` (a deliberate disable — safe,
since `MinRecallFloor` still applies) silently becomes 5s, and negatives likewise. The zero case is
unfixable without pointer fields; the negative case is unambiguously a mistake, and the package
rejects every other unambiguous mistake. Fix: `< 0` errors from `validate()`, `== 0` keeps
default-fill (documented as "default", not "off").

**L16. Only the first YAML document is decoded.** `config.go:158-165`. `scene_name: a\n---\nscene_name: b\n`
loads as `a` with no error. `KnownFields(true)` exists precisely so a config line can never be a
silent no-op; a dropped document is the same failure at a larger granularity. Fix: a second `Decode`
into a discard, erroring unless it returns `io.EOF`.

**L17. `Registry.Sync` inserts resources with empty IDs; `applyOne` rejects them.**
`registry.go:69,77,85,93,101,109` vs `:187,199,219,246,269`. The stream path guards `in.ID == ""` on
every type; the sync path guards none, so a resource returned without an id lands under the `""` key,
making `Group("")`/`SmartScene("")` return `ok=true` for a zero-value id. Combines badly with M6.
Only the downstream `scene.Group.RID != group.ID` check saves it.

**L18. `CleanAddress` accepts hosts and ports it exists to reject.** **(verified)** `config.go:266-273`.
The port check is `strconv.Atoi`, which accepts `-1`, `0`, `99999`; a bare host is never validated, so
`address: "hello world"` passes. All are spliced into `"https://" + addr` and fail later with a
`net/url` error — the "baffling errors much later" the function's own doc comment says it prevents.
Fix: `strconv.ParseUint(port, 10, 16)` with a non-zero check, plus host validation.

**L19. Blank exclusion entries vanish with no diagnostic.** `config.go:355-357`, `:373-375`.
`if key == "" { continue }` drops whitespace-only entries before the matching loop, so they never
reach `Unmatched`. `exclude.rooms: ["  "]` yields `Unmatched=[] Ineffective=[]` — a config line that
protects nothing and says nothing, in the one package that otherwise reports every inert entry.

**L20. A corrupt or zero-length credentials file is an unrecoverable hard error.** `credentials.go:36-38`.
`json.Unmarshal` on an empty file returns `unexpected end of JSON input`, which `loadAll` propagates
(`main.go:277-280`), aborting `run` *and* `auth` — so `hue-scene-keeper auth`, the documented repair
path, cannot heal the file. The user must delete it by hand and the error never says so. Contrast the
missing-file path, which yields a helpful `ErrNoCredentials`.

**L21. The "refuse to overwrite with a blank key" guard fails open exactly when it matters.**
**(verified)** `credentials.go:55-60`. `if existing, err := LoadCredentials(c.path); err == nil &&
existing.AppKey != ""` — any read error (corrupt JSON, mode 0000, foreign ownership) makes the guard
skip and the blank key gets written. Confirmed: saving a blank-key `Credentials` over a mode-0000 file
containing a real key succeeds and leaves `{"app_key": ""}`. Currently unreachable from the CLI, so
latent — but it is a safety interlock that unlocks on failure. Fix: fail closed on any error other
than `os.IsNotExist`.

**L22. `LoadCredentials` never checks the file's mode.** **(verified)** `credentials.go:27-41`. `Save`
is meticulous about 0600, but a file restored from a backup or written by an older version at 0644
loads without a murmur. Given how much of the design is about that 0600, an ssh-style warning costs
one `os.Stat`.

**L23. Derived indexes keep a deleted light.** `registry.go:148-156`, `:390-396`. `lightToZones` is
rebuilt from zone `Children`, which still list a deleted light until the zone itself updates. After
`delete` on `light1`, `GroupForLight("light1")` returns `zone1` with `ok=true` though `r.lights` no
longer has it; `GroupShadowed("zone1")` flips to `false`, suppressing the "exclusion has no effect"
warning the config layer depends on (`config.go:387-396`). Self-heals at the next full `Sync`; impact
limited to a wrong diagnostic.

**L24. One unparseable resource fails the entire sync, permanently.** `registry.go:119-123`. Any
`f.into` error aborts `Sync` → fails `Resync` → fails `onConnect` → drops the stream, and the same bad
resource is refetched on every reconnect, so the daemon backs off to the 10-minute cap and stays
alive, connected-then-dropped, doing nothing forever. Needs a type change from the bridge (e.g.
`children` arriving as an object), so likelihood is low — but blast radius is total. Fix: skip and log
the individual resource; fail the sync only on an HTTP-level error or a wholly empty resource type.

### CLI / service / packaging

**L25. No second-signal force quit.** `main.go:134`. `signal.NotifyContext`'s goroutine exits after the
first signal, but the `Notify` registration lives until `stop()` runs at `run()`'s return, so default
disposition stays disabled and a second Ctrl-C/SIGTERM does nothing at all. Harmless today because
shutdown is prompt — but it bites the moment M13's blocking drain is added.

**L26. `auth` interrupted with Ctrl-C exits 0 and prints nothing.** `main.go:46-49` with `:327-334`.
`PairWithRetry` wraps `ctx.Err()`, so an interrupted pairing returns an error that
`errors.Is(err, context.Canceled)` swallows: no output, exit status 0. A provisioning script running
`hue-scene-keeper auth || exit 1` concludes pairing succeeded. The blanket unwrap is right for `run`,
wrong for the interactive subcommands.

**L27. Invalid `--log-level`/`--log-format` fall back silently.** `main.go:169-180`.
`--log-level=verbose` gives info with no complaint; `--log-format=jsn` gives text. Everything else in
this CLI treats a typo as an error (README:111-113 advertises exactly that).

**L28. `resolve` syncs the registry twice, the second time with no timeout.** `main.go:491-497`.
`prepare` already does a full `reg.Sync` under a 30s deadline (`:367-371`); `k.Resync(ctx)`
immediately repeats it against the *root* context, so a wedged bridge leaves `resolve` hanging
indefinitely, at the cost of a second full pass of rate-limited GETs.

**L29. The documented manual macOS install has no `launchctl enable`.** `README.md:140-142` and
`deploy/dev.staub.hue-scene-keeper.plist:13-14`. `hue-scene-keeper stop` writes the label into the
per-user disabled database (`service_darwin.go:85`). A user who then reinstalls by hand with the
documented `cp` + `launchctl bootstrap` hits `Bootstrap failed: 5: Input/output error` with no way to
guess why — the enable-first rule is honoured in `postinstall` and `service.Start`, but not in the two
places a human copies commands from.

**L30. `pkg:uninstall` leaves the label in launchd's disabled database.** `magefiles/magefile.go:268-281`.
`bootout` + `rm plist` + `pkgutil --forget` do not clear the disabled entry, and that database
outlives the plist. A pkg reinstall recovers (postinstall enables first); a hand reinstall does not —
see L29. Fix: add `launchctl enable gui/$(id -u)/<label>` alongside the best-effort `bootout`.

**L31. A `launchctl` failure aborts postinstall and reports a failed install that actually succeeded.**
`deploy/scripts/postinstall:15,45-47`. Under `set -eu`, a failing `enable` or `bootstrap` — classically
`bootout` returning before launchd finished tearing the job down, so the following `bootstrap` races
it — exits non-zero after binary and plist are already in place. The user sees "The installation
failed" and reinstalls, when `RunAtLoad` would have picked the agent up at next login. Fix: retry the
final `bootstrap` 2-3 times, then `|| echo "agent will start at next login"`.

**L32. `RestrictAddressFamilies` omits `AF_NETLINK`, which breaks DNS in a cgo-built binary.**
`deploy/hue-scene-keeper.service:55` with `magefiles/magefile.go:92-94` vs `:148`. `Go.Build` — what
the unit header at line 4 tells the admin to install — does not set `CGO_ENABLED=0`, unlike
`buildTo`, so on a machine with a C toolchain the binary uses glibc's `getaddrinfo`, which opens an
`AF_NETLINK` socket. Only cloud discovery resolves a hostname, so blast radius is small, but the
failure is a confusing `no Hue bridge found - cloud: ...` on a host where DNS demonstrably works.
Fix: add `AF_NETLINK`, or set `CGO_ENABLED=0` in `Go.Build` so local builds match shipped ones.

**L33. Missing credentials leave the systemd unit permanently failed, undocumented.**
`deploy/hue-scene-keeper.service:29-30,40-41`, `README.md:157-160`. `Restart=always` + `RestartSec=5s`
+ `StartLimitBurst=10`/`300s` means enabling before pairing burns the burst in ~50s and lands in
`start-request-repeated-too-quickly`; after pairing, `systemctl start` still refuses until
`systemctl reset-failed`. The README's self-recovery promise ("it picks them up on its own, with
nothing to restart") is true only for launchd. The rate limit is correct — the gap is documentation.

**L34. `go:install` runs the compiler under sudo.** `magefiles/magefile.go:114-118`, prescribed as
`sudo -E go tool mage go:install` in the unit and plist headers. `mg.Deps(Go.Build)` re-runs `go build`
as root with `-E` preserving `HOME`, so root-owned entries land in the user's `GOCACHE`/module cache
and later non-root builds fail with permission errors. Also fails outright if `/usr/local/bin` does not
exist (`install` without `-d`). Fix: mirror `Pkg.Install` — build as the user, then
`sudo install -d -m0755 ...`.

**L35. `lipo` failures are silent about why.** `magefiles/magefile.go:215` uses `sh.Run` (output
discarded) where every other step uses `sh.RunV`. Without Xcode CLT the user gets a bare exit-status
error mid-`pkg:build`.

**L36. `status` always exits 0.** `service_darwin.go:100-142`, `service_linux.go:92-125`. Deliberate for
humans, but useless in a script or monitoring check, and there is no machine-readable form (`--json`
is list-only). Fix: LSB convention (0 running, 3 not running) or add `--json`.

**L37. Flag errors are printed twice.** **(verified)** `main.go:104-109,124-129`. `flag.ContinueOnError`
already prints the message and usage; `main` prints `error: flag provided but not defined: -nope`
again below the usage block. Cosmetic.

### Test-only

**L38. Test mutation of the package global `healthyConnection` races leaked `Stream` goroutines.**
`eventstream.go:18` and `:102`, mutated at `eventstream_test.go:210-211`, `:249-250`, `:294-295`. Three
tests spawn `go func() { _ = c.Stream(ctx, …) }()` (lines 227, 265, 302) and never join; the deferred
`cancel()` runs before `t.Cleanup` restores the global but does not wait for the goroutine. The read
at `:102` happens *after* the `ctx.Err()` check at `:92`, so a cancel landing in that window lets the
goroutine read while cleanup writes — and it can outlive the test into the next one. The race detector
misses it because the window is a few instructions wide. Fix: a `done` channel joined in cleanup;
better, move `healthyConnection` into `StreamOptions`.

**L39. The fake calls `t.Fatalf`/`t.Errorf` from non-test goroutines.** `fake/fake.go:184`, `:234`,
`:249`, `:269`, reached via `handlePut` → `go b.echoGroupOn(groupID)` (`:443`). `t.FailNow` must be
called from the test goroutine; from `echoGroupOn` it calls `runtime.Goexit` on the wrong one. Worse,
`echoGroupOn` is spawned *after* the response is written, so `httptest.Server.Close` does not wait for
it — a `broadcast` hitting its 2s timeout after the test finished panics the binary with "Log in
goroutine after test has completed". Latent flake in `TestDoesNotLoopWhenRecallTurnsOnWholeRoom`, the
suite's most important test. Fix: record failures into a mutex-guarded slice drained in `t.Cleanup`,
and have `NewBridge` wait for outstanding echo goroutines.

**L40. `time.After` inside a loop in the fake bridge.** `fake/fake.go:268`. `broadcast` allocates a 2s
timer per subscriber per published frame; unreferenced but live to expiry. Immaterial at test volumes,
noted only because it is the classic shape.

### Coverage gaps

`go test ./internal/keeper -coverprofile` reports 88.7%. These lines are executed by no test:

- `keeper.go:667-668` — `drain`'s in-flight deferral, and therefore **`later()` (`:700-705`) entirely**.
  No test puts a second trigger on a group whose recall is on the wire. This is also the branch that
  would hot-spin (timer fires → not ready → re-arm at +0) if `CoalesceWindow` were ever 0;
  `config.applyDefaults` guarantees it is not for any loaded config, but `New` applies no defaults of
  its own, so a hand-built `&config.Config{}` reaches it.
- `keeper.go:944`, `:949` — the *restore-a-previous-value* halves of `rollback`. Every rollback test
  runs on a group with no prior `lastRecall`/`suppressUntil`, so only the `delete` branches execute.
  Both branches read as correct, but the invariant they protect is untested — and M5 turns on exactly
  this code.
- `keeper.go:1014-1019` — `finish`'s "superseded by a newer trigger" path.
- `keeper.go:930-932` — `commit`'s `sendQ`-full rollback.
- `keeper.go:547` — `pump`'s full-queue `return`.
- `keeper.go:598` — `kindActivity` for a light in no group.
- `keeper.go:765` — `lookupWorker`'s `ctx.Done` during the `lookupsDone` send.
- `keeper.go:871` — a *valid* `smart_scene_overrides` entry (only the two failure modes are tested).

Not exercised under `-race` at all: the entire `cmd/hue-scene-keeper` package (no test files, so
`newClient`'s `OnLearn` closure, signal handling, and `cmdRun` wiring are untested); the cert-pinning
path (every test client sets `Insecure: true`); `hue.Discover`/`probeAll`/`discoverMDNS` (only the
skipped `TestManualDiscovery`); and `internal/service` exec paths on the non-host platform.

---

## Verified correct — checked, no action needed

Recorded so this ground is not re-covered.

**The central concurrency invariant holds.** Every read and write of `suppressUntil`, `lastRecall`,
`inFlight`, `warnedNoScene`, and `seq` was traced; all are reached only via `dispatch` →
{`schedule`→`resolve`/`extend`, `drain`→`commit`, `finish`→`rollback`, `queueDeviceLookup`}. The
`*pendingRecall` handover is correct: `drain` deletes from `pending` *before* `commit`, `commit`
writes `prevRecall`/`prevSuppress` *before* the `sendQ` send (the send is the happens-before edge),
and afterwards dispatch touches only `inFlight` and a local copy. `finish` re-adopts the pointer only
after the outcome round-trip and guards against a newer entry holding the slot. **No unsynchronized
sharing in the daemon.**

- **Timer discipline** (`keeper.go:503-531`): `stopTimer` nils `timerC` so a stale fire can never be
  read, and `arm` always allocates a fresh `time.Timer` rather than `Reset`-ing — the pattern that
  avoids the wrong-fire bug. `arm()` is called on exactly the paths that mutate `pending`. Inductively,
  `pending` non-empty ⇒ `timerC != nil`. `arm` recomputes the global minimum, so a later-arriving
  earlier deadline is handled.
- **Boundary conditions**: `drain` `!fireAt.After(now)`, `resolve` `now().Before(until)`, `commit`
  `now.Sub(last) < minInterval`, `extend` `!next.After(fireAt)` — all consistent and correct at the
  boundary. `earlier`/`later` are correct.
- **`inFlight` bookkeeping**: set only on a successful enqueue; `sender` always produces an outcome
  except on ctx cancellation (where dispatch is gone); `finish` deletes unconditionally. Double-delete
  is a no-op. A rollback clearing a *later* recall's suppression was attempted and could not be
  constructed — it needs two `commit`s for one group with no intervening `finish`, and `drain` guards
  on `inFlight`.
- **Channel discipline**: no channel is ever closed in production code, so no double-close or
  send-on-closed is possible. Every potentially-blocking send has a `<-ctx.Done()` arm;
  `emit`/`pump`/`commit` use non-blocking sends. `lookupsDone`'s capacity equals the max number of
  workers that can hold an item, so it can never block.
- **Shutdown**: `workers.Add` before `go`; `Stream` provably returns only on `ctx.Err()`, so
  `workers.Wait()` is never reached while a worker could still be scheduling. No reconnect can start
  mid-shutdown. The absence of an overall shutdown deadline is safe today.
- **Contexts**: none stored in a struct; every `WithCancel`/`WithTimeout` has a matching
  `defer cancel()`; `probeAll`'s goroutines write distinct slice indices under a `WaitGroup`.
  `go vet` (including `lostcancel`) is clean.
- **`limiter`** (`client.go:149-210`): correct even spacing, no burst; the reserve/release protocol is
  right in both cancellation orderings, and the documented single-slot leak is in the safe direction.
  Dead-context check before reserving, timer always stopped, no deadlock or leak.
- **`do()`** (`client.go:297-345`): body closed on every path including non-2xx; response capped by
  `io.LimitReader`; `bytes.NewReader` gives the request a `GetBody` so transport replays are safe; the
  `var rdr io.Reader` nil-interface pattern is correct. Deadline placement after `lim.wait` matches the
  documented invariant exactly.
- **`Pin.verify` logic** (`client.go:116-147`): runs during the handshake, before any request bytes
  leave; empty chain rejected; parse error fails closed; leaf index correct; pin is over
  `RawSubjectPublicKeyInfo`; `OnLearn` failure aborts rather than silently trusting; concurrent
  handshakes serialise on `p.mu`. `ClientSessionCache` is nil, so no resumption path skips
  verification. `InsecureSkipVerify: true` does disable hostname checking, but for a self-signed LAN
  bridge reached by IP the SPKI pin is the stronger check — correct as designed.
- **`parseARecords`/`skipName`** (`discover.go:309-376`): every slice index bounds-checked;
  `skipName` cannot loop; reserved label types fail closed; `net.IP(...).String()` copies out of the
  reused buffer so there is no aliasing. No memory-safety issue despite untrusted input.
- **SSE framing**: comments, blank-line dispatch, multi-line `data` joining, split-across-writes
  reassembly, and unterminated trailing frames all handled correctly. Only deviation: `consumeLine`
  (`:228-230`) drops a leading empty `data:` field — harmless for JSON payloads.
- **`MinRecallFloor`** is applied on every path that can set the interval: `Default()` seeds it
  (`config.go:134`) and `applyDefaults` clamps after decode (`:192-194`), including negative and unset
  values. Confirmed for `-5s`, `1s`, missing file, empty file, `null`, `{}`, `---`, and comment-only files.
- **Config loading**: missing/empty/comment-only/`null`/`{}` all yield a fully-defaulted valid config
  with no error. `KnownFields(true)` catches unknown keys at top level and inside `bridge:`/`exclude:`;
  yaml.v3 rejects duplicate mapping keys by default.
- **Credentials atomic write** is correct: `CreateTemp` in the target directory (same device), 0600
  from creation, cleanup on every error path, file synced before rename, directory synced after.
- **Registry locking**: fully `RWMutex`-guarded; `Sync` builds a detached `Registry` and swaps under
  the write lock. No lock upgrades; `GroupShadowed` uses only `*Locked` helpers under `RLock`. No
  `Sync`/`Apply` race — `OnConnect` runs on the stream goroutine before the read loop starts, so no
  events are lost across the cache swap.
- **`exclude.rooms` vs `exclude.lights`** are kept distinct throughout.
- **`overrideFor`**'s id-beats-name-then-lexicographic tiebreak is deterministic across all four
  byID/bestByID combinations.
- **XDG handling**: empty vs unset `XDG_*_HOME` both fall through to the home-relative default, per spec.
- **Test-only hooks are not reachable in production**: `Options.Insecure`/`BaseURL` are set only by
  the fake and hue tests — `newClient` (`main.go:264-270`) sets neither and no config or flag path
  feeds them. `k.now`/`k.onRecalled` are unexported and set only in `keeper_test.go`, before `Run` or
  through an `atomic.Int64`. (Noted as a latent risk only: nothing but review stops a future call site
  from setting `Insecure`.)
- **Packaging**: the `auth`/daemon path contract on Linux (unit passes `--config`/`--state`
  explicitly); `launchctl enable` before `bootstrap` in `postinstall:46-47`; the `/bin/sh -c` `$HOME`
  expansion in the plist; `signal.NotifyContext`'s channel is buffered;
  `StateDirectory=hue-scene-keeper` matches the `--state` path under `ProtectSystem=strict`;
  `Discover` never returns `(empty, nil)` so `bridges[0]` cannot panic; no path logs the app key or
  pin at any level; `launchctl enable/disable gui/$UID/...` does not need root (verified empirically).
