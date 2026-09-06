// Package keeper watches the bridge's event stream and puts a room back into
// its all-day smart scene whenever a light in it comes on.
package keeper

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/config"
	"github.com/csstaub/hue-scene-keeper/internal/hue"
	"github.com/csstaub/hue-scene-keeper/internal/registry"
)

// Trigger reasons, used in logs.
const (
	reasonSwitchedOn    = "light_switched_on"
	reasonPowerRestored = "power_restored"
	reasonStartup       = "startup"
	reasonReconnect     = "came_on_while_disconnected"
)

// maxDeviceLookups bounds how many power-restore lookups run at once. They are
// deliberately off the dispatch goroutine, but they still share the bridge's
// rate limiter, so a whole-house restore must not become a stampede of
// concurrent GETs.
//
// The bound is on concurrency alone. Work that does not fit waits its turn: a
// fuse or a breaker spanning several rooms is precisely the case Trigger B
// exists for, and dropping the rooms that arrive after the first few would lose
// most of it.
const maxDeviceLookups = 4

// deviceLookupQueue is the depth of the queue those workers drain. Lookups are
// coalesced by group and only one per group is ever outstanding, so this is far
// deeper than any real bridge can fill; dispatch holds the overflow rather than
// discarding it if it ever does.
const deviceLookupQueue = 256

// recallQueue is the depth of the queue between dispatch and the sender. One
// entry per group is the most that can ever be outstanding, so this is deeper
// than any real bridge has rooms.
const recallQueue = 256

// recallBackoff is the delay before each retry of a refused recall. Its length
// is the number of retries, so the total number of attempts is one more than
// that: every element is used, and adding one adds an attempt. A recall the bridge rejected for a transient reason is
// worth repeating, because the alternative is a room left in whatever state
// something else put it in until one of its lights is next switched on by hand.
//
// The delays are fixed rather than jittered: the client's rate limiter already
// spaces retries out, so a house-wide wave of them queues rather than collides.
var recallBackoff = []time.Duration{time.Second, 3 * time.Second}

type triggerKind int

const (
	// kindLight is a specific light that just came on.
	kindLight triggerKind = iota
	// kindDevice is a device whose radio just came back; which of its lights
	// are actually on must still be read from the bridge.
	kindDevice
	// kindActivity is a light being written to without coming on: a
	// brightness or colour change, or a light going off. It is never a reason
	// to recall anything. It is evidence that something else is still
	// changing the room, which is a reason for a recall already waiting on
	// that room to keep waiting.
	kindActivity
)

type trigger struct {
	kind   triggerKind
	id     string
	reason string
}

// deviceLookup is one unit of power-restore work: the lights of a single group
// that still need reading from the bridge. The unit is a group rather than a
// device because a group is recalled as a whole, so however many of its lamps
// rejoined at once, one light found on is all the answer we need.
type deviceLookup struct {
	groupID  string
	lightIDs []string
	reason   string
}

// recall is a resolved, ready-to-send action.
type recall struct {
	groupID   string
	groupName string
	sceneID   string
	sceneName string
	lightName string
	reason    string
}

// pendingRecall is a recall waiting for its group to fall quiet.
//
// It belongs to the dispatch goroutine while it sits in the pending map, and
// to the sender between being handed to sendQ and its outcome coming back.
// Only one of the two ever holds it, so it needs no lock of its own.
type pendingRecall struct {
	recall

	// seq is the order this group became pending, and so the order its recall
	// is sent in: the room you walked into first is styled first.
	seq uint64

	// fireAt is the deadline; activity in the group pushes it out. hardAt is
	// where that pushing stops, fixed when the entry is created.
	fireAt time.Time
	hardAt time.Time

	// attempt counts the retries already spent on this recall.
	attempt int

	// foldedPowerRestore records that a power_restored trigger folded into an
	// entry created by some other reason. It is sticky and never cleared: one
	// lamp coming back on mains is reason enough to restyle the room however
	// the entry started life.
	foldedPowerRestore bool

	// The suppression state to put back if the bridge refuses this recall.
	// It has to be carried on the entry because arming and finding out are no
	// longer the same moment - a request sits on the wire in between.
	//
	// prevSuppress covers every group commit armed, which for a zone recall is
	// more than one; armedUntil is the deadline it wrote to all of them, so
	// rollback can tell its own window from a newer one.
	prevRecall   time.Time
	hadRecall    bool
	armedUntil   time.Time
	prevSuppress []groupSuppression
}

// groupSuppression is one group's suppression deadline as commit found it,
// kept so rollback can put back exactly what was overwritten.
type groupSuppression struct {
	groupID string
	until   time.Time
	had     bool
}

// skipsOnCheck reports whether commit's "is any light still on" veto does not
// apply, because a lamp somewhere in the group came back on mains.
//
// A power_restored lookup deliberately never writes what it read back into the
// registry, so the registry has no record of the lamp returning and would veto
// every restore. Both sources count: the reason this entry was created for,
// and any power_restored trigger that folded into it afterwards.
func (p *pendingRecall) skipsOnCheck() bool {
	return p.foldedPowerRestore || p.reason == reasonPowerRestored
}

// outcome carries a sent recall back to dispatch, which owns every piece of
// state that has to change in response to it.
type outcome struct {
	pending *pendingRecall
	err     error
}

// Keeper owns the event loop. Construct it with New and run it with Run.
type Keeper struct {
	client *hue.Client
	reg    *registry.Registry
	log    *slog.Logger

	mu   sync.RWMutex
	cfg  *config.Config
	excl *config.Exclusions

	dryRun bool

	// started guards the one-shot startup pass; only touched on the stream
	// goroutine, which is where onConnect runs.
	started bool

	// Owned exclusively by the dispatch goroutine; no locking needed. That
	// stays true even though recalls are now sent from another goroutine:
	// the sender is handed a pendingRecall and gives back an outcome, and
	// every decision made from these maps happens on dispatch either side of
	// that handover.
	suppressUntil map[string]time.Time
	lastRecall    map[string]time.Time
	warnedNoScene map[string]bool
	inFlight      map[string]bool
	seq           uint64

	triggers chan trigger
	// sendQ carries committed recalls to the sender; results carries what the
	// bridge said back to dispatch.
	sendQ   chan *pendingRecall
	results chan outcome
	// deviceLookups carries coalesced power-restore work to the lookup
	// workers; lookupsDone carries the group id back when a worker is finished
	// with it, so dispatch - which owns the coalescing state - can release it.
	deviceLookups chan deviceLookup
	lookupsDone   chan string

	// Hooks for tests.
	now        func() time.Time
	onRecalled func(recall)
}

// New builds a Keeper.
func New(client *hue.Client, reg *registry.Registry, cfg *config.Config, log *slog.Logger, dryRun bool) *Keeper {
	if log == nil {
		log = slog.Default()
	}
	// A nil config is a caller's mistake, but the dispatch goroutine is the
	// wrong place to find it out: it panics there seconds later, on a stack
	// that says nothing about who built the Keeper. Falling back to the
	// defaults is not papering over a misconfiguration - they are the very
	// settings a missing config file yields, which is a supported state.
	if cfg == nil {
		cfg = config.Default()
	}
	return &Keeper{
		client:        client,
		reg:           reg,
		log:           log,
		cfg:           cfg,
		dryRun:        dryRun,
		suppressUntil: map[string]time.Time{},
		lastRecall:    map[string]time.Time{},
		warnedNoScene: map[string]bool{},
		inFlight:      map[string]bool{},
		triggers:      make(chan trigger, 512),
		sendQ:         make(chan *pendingRecall, recallQueue),
		// Deep enough that the sender never waits on dispatch to be read.
		results:       make(chan outcome, recallQueue),
		deviceLookups: make(chan deviceLookup, deviceLookupQueue),
		// Deep enough that a worker reporting a finished group never waits on
		// dispatch, which may be blocked inside a recall's HTTP round trip.
		lookupsDone: make(chan string, maxDeviceLookups),
		now:         time.Now,
	}
}

// streamGiveUp is how many consecutive stream failures end Run with
// hue.ErrStreamUnreachable instead of retrying forever.
//
// With the stream's linear backoff that is a little over two minutes of
// trying, comfortably longer than a bridge reboot, and short enough that a
// bridge which moved to a new DHCP lease is rediscovered rather than waited on
// for the rest of the daemon's life.
const streamGiveUp = 12

// Run syncs the registry, then consumes the event stream until ctx is
// cancelled. It returns ctx.Err() on shutdown, or hue.ErrStreamUnreachable if
// the bridge stopped answering at the address it was given.
func (k *Keeper) Run(ctx context.Context) error {
	// Dispatch and the lookup workers all stop on ctx, and Stream only returns
	// once ctx is cancelled, so waiting here is what makes Run's return mean
	// "nothing of mine is still running" - including the network calls a
	// power-restore lookup may be part-way through.
	var workers sync.WaitGroup
	start := func(name string, fn func(context.Context)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer k.logPanic(name)
			fn(ctx)
		}()
	}
	start("dispatch", k.dispatch)
	start("sender", k.sender)
	for range maxDeviceLookups {
		start("lookup", k.lookupWorker)
	}

	err := k.client.Stream(ctx, hue.StreamOptions{
		Logger:                 k.log,
		OnConnect:              k.onConnect,
		MaxConsecutiveFailures: streamGiveUp,
	}, k.HandleEvents)

	workers.Wait()
	return err
}

// logPanic reports a panic on one of Run's goroutines through the daemon's own
// logger, naming which one it was, and then lets it carry on unwinding.
//
// It deliberately does not absorb the panic. Every one of these goroutines is
// load-bearing: dispatch is where recalls are decided at all, and a lookup
// worker that dies mid-item leaves its group marked busy forever, so its room
// never gets another power-restore lookup. Swallowing the panic would leave the
// daemon running, logging nothing further, and quietly doing none of that -
// exactly the failure the stream's give-up behaviour exists to avoid. Dying is
// what gets the service manager to restart us; the log line is so the next
// person knows which goroutine went and why.
func (k *Keeper) logPanic(name string) {
	r := recover()
	if r == nil {
		return
	}
	k.log.Error("keeper goroutine panicked",
		"goroutine", name, "panic", r, "stack", string(debug.Stack()))
	panic(r)
}

// onConnect resyncs after every (re)connection and works out what we missed.
//
// Returning an error drops the connection so the stream's backoff retries it.
// Without that a transient resync failure would leave the daemon running
// against an empty cache on a connection that never drops - alive, silent, and
// doing nothing until restarted.
func (k *Keeper) onConnect(ctx context.Context) error {
	// Snapshot before the resync overwrites it, so we can tell what changed
	// while we were blind: which lights came on, and which lamps got their
	// radio back.
	before := k.reg.OnLightIDs()
	beforeDown := k.reg.DisconnectedDeviceIDs()
	first := !k.started

	if err := k.Resync(ctx); err != nil {
		// A resync cut short by shutdown is not a failure; saying so at ERROR
		// puts a scary line in the log for every ordinary restart.
		if ctx.Err() == nil {
			k.log.Error("resync failed, dropping the connection to retry", "err", err)
		}
		return fmt.Errorf("resync after connect: %w", err)
	}
	k.started = true

	if first {
		k.mu.RLock()
		applyOnStartup := k.cfg.ApplyOnStartup
		k.mu.RUnlock()
		if applyOnStartup {
			k.enqueueStartupTriggers()
		}
		return nil
	}

	// A reconnect. A light that came on while we were disconnected produces
	// no off->on edge - the resync simply records it as already on - so it
	// would otherwise never be recalled. Diffing against the snapshot finds
	// exactly those lights, without acting on ones that were already on.
	after := k.reg.OnLightIDs()
	var newly []string
	for id := range after {
		if !before[id] {
			newly = append(newly, id)
		}
	}
	sort.Strings(newly)
	if len(newly) > 0 {
		k.log.Info("lights came on while disconnected", "count", len(newly))
	}
	for _, id := range newly {
		k.emit(trigger{kind: kindLight, id: id, reason: reasonReconnect})
	}

	// Trigger B has the same hole, and until now no recovery for it. It fires
	// only on a stream event carrying "connected", so a lamp whose mains came
	// back while we were disconnected is simply recorded as connected by the
	// resync, as though it had always been so, and is never recalled. The
	// second snapshot finds exactly those devices. A device that has vanished
	// from the bridge entirely also leaves the set, but the lookup that follows
	// finds no lights for it and quietly does nothing.
	afterDown := k.reg.DisconnectedDeviceIDs()
	var restored []string
	for id := range beforeDown {
		if !afterDown[id] {
			restored = append(restored, id)
		}
	}
	sort.Strings(restored)
	if len(restored) > 0 {
		k.log.Info("devices regained connectivity while disconnected", "count", len(restored))
	}
	for _, id := range restored {
		k.emit(trigger{kind: kindDevice, id: id, reason: reasonPowerRestored})
	}
	return nil
}

// Resync refreshes the resource cache and re-resolves the exclusion lists
// against it, logging any config entry that matched nothing.
func (k *Keeper) Resync(ctx context.Context) error {
	if err := k.reg.Sync(ctx, k.client); err != nil {
		return err
	}
	k.mu.Lock()
	k.excl = config.ResolveExclusions(k.cfg, k.reg)
	excl := k.excl
	k.mu.Unlock()

	lights, rooms, zones, scenes := k.reg.Counts()
	k.log.Info("synced bridge resources",
		"lights", lights, "rooms", rooms, "zones", zones, "smart_scenes", scenes)

	for id, name := range excl.GroupNames {
		k.log.Info("excluded group (never recalled)", "group", name, "id", id)
	}
	for id, name := range excl.LightNames {
		k.log.Info("excluded light (never triggers)", "light", name, "id", id)
	}
	for _, entry := range excl.Unmatched {
		k.log.Warn("config exclusion matched nothing", "entry", entry)
	}
	for _, entry := range excl.Ineffective {
		k.log.Warn("config exclusion has no effect", "entry", entry)
	}
	for _, entry := range excl.OverrideProblems {
		k.log.Warn("smart scene override problem", "entry", entry)
	}
	return nil
}

// enqueueStartupTriggers queues one trigger per group that has a light on.
//
// Exclusions are consulted here rather than left to resolve(), because a group
// gets one trigger: if that trigger were spent on an excluded light, the group
// would be dropped even though other lights in it are on.
func (k *Keeper) enqueueStartupTriggers() {
	k.mu.RLock()
	excl := k.excl
	k.mu.RUnlock()

	for _, group := range append(append([]hue.Group{}, k.reg.Rooms()...), k.reg.Zones()...) {
		if excl.GroupExcluded(group.ID) {
			continue
		}
		for _, lightID := range k.reg.LightIDsInGroup(group.ID) {
			if excl.LightExcluded(lightID) {
				continue
			}
			if on, known := k.reg.LightIsOn(lightID); known && on {
				k.emit(trigger{kind: kindLight, id: lightID, reason: reasonStartup})
				break
			}
		}
	}
}

// HandleEvents feeds a batch of stream events through the registry and the
// trigger detectors. It is called on the stream reader goroutine.
func (k *Keeper) HandleEvents(events []hue.Event) {
	for _, ev := range events {
		if ev.Type == "error" {
			k.log.Warn("bridge reported a stream error event", "id", ev.ID)
			continue
		}
		for _, raw := range ev.Data {
			k.handleResource(ev.Type, raw)
		}
	}
}

// handleResource merges one resource and detects the transitions we act on.
//
// The prior state is read from the registry immediately before the merge.
// That is safe because the registry is written only from this goroutine -
// an invariant worth preserving: nothing else may call merge().
func (k *Keeper) handleResource(action string, raw json.RawMessage) {
	switch hue.PeekType(raw) {
	case hue.TypeLight:
		var in hue.Light
		if err := json.Unmarshal(raw, &in); err != nil || in.ID == "" {
			return
		}
		priorOn, priorKnown := k.reg.LightIsOn(in.ID)
		k.merge(action, raw)

		if action == "delete" {
			return
		}
		// Trigger A: off -> on. A light we have never seen counts too; that
		// only happens for a genuinely new light, where acting is correct.
		if in.On == nil || !in.On.On || (priorKnown && priorOn) {
			// Not an edge, but still somebody writing to this light: a
			// brightness or colour change, a light going off, or a light
			// being turned on that was already on. If its group is waiting to
			// be recalled, that wait restarts. This is what stops us styling
			// a room a home automation has not finished with - the late half
			// of its commands would otherwise land on top of our scene, and
			// carry no off->on edge to tell us it had happened.
			k.emitActivity(in.ID)
			return
		}
		k.emit(trigger{kind: kindLight, id: in.ID, reason: reasonSwitchedOn})

	case hue.TypeZigbeeConnectivity:
		var in hue.ZigbeeConnectivity
		if err := json.Unmarshal(raw, &in); err != nil || in.ID == "" {
			return
		}
		prior, priorKnown := k.reg.Connectivity(in.ID)
		k.merge(action, raw)

		if action == "delete" || in.Status != hue.StatusConnected {
			return
		}
		// Trigger B: mains power came back.
		//
		// This is the case Trigger A cannot see. When a lamp's power is cut
		// while the bridge stays up, our cached state still says "on"; when
		// the lamp returns it is on again, so there is no off->on edge to
		// detect. Connectivity is the only signal.
		if !priorKnown || prior.Status == "" || prior.Status == hue.StatusConnected {
			return
		}
		device := in.Owner.RID
		if device == "" {
			device = prior.Owner.RID
		}
		if device == "" {
			return
		}
		k.emit(trigger{kind: kindDevice, id: device, reason: reasonPowerRestored})

	default:
		k.merge(action, raw)
	}
}

// merge writes to the registry. Stream goroutine only - see handleResource.
func (k *Keeper) merge(action string, raw json.RawMessage) {
	k.reg.Apply([]hue.Event{{Type: action, Data: []json.RawMessage{raw}}})
}

// emit queues a trigger without blocking the caller. Dropping under extreme
// load is better than stalling the stream reader and losing events entirely.
func (k *Keeper) emit(t trigger) {
	select {
	case k.triggers <- t:
	default:
		k.log.Warn("trigger queue full, dropping", "id", t.id, "reason", t.reason)
	}
}

// emitActivity queues a debounce extension, yielding the back half of the
// queue to real triggers.
//
// Activity is much the more numerous of the two - every brightness step of
// every light produces one - and much the less important: losing a trigger
// loses a recall, while losing an activity only lets a recall land sooner
// than it might have. So it competes for the queue only while there is room
// to spare.
func (k *Keeper) emitActivity(lightID string) {
	if len(k.triggers) >= cap(k.triggers)/2 {
		return
	}
	k.emit(trigger{kind: kindActivity, id: lightID})
}

// dispatch schedules recalls and decides what to do with their outcomes. It
// also owns the power-restore lookup queue, for the same reason it owns the
// suppression windows: one goroutine holding all the per-group state needs no
// locking.
//
// A group's recall waits for the group to fall quiet rather than for a fixed
// window. See extend() for why.
func (k *Keeper) dispatch(ctx context.Context) {
	pending := map[string]*pendingRecall{}
	// Power-restore work, keyed by group. queued holds groups whose lights
	// still need reading, busy the groups a worker currently has, and a group
	// is never in both: that is what turns a whole-house restore into one
	// lookup per room instead of one per lamp. Because busy is cleared when the
	// worker reports back, a room coalesced away now is looked up again the
	// next time its power really does come back.
	queued := map[string]*deviceLookup{}
	busy := map[string]bool{}

	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		timerC = nil
	}
	defer stopTimer()

	// arm points the timer at the earliest deadline in pending. Deadlines
	// move whenever a group sees activity, so it is recomputed after every
	// change rather than set once per batch.
	arm := func() {
		stopTimer()
		var first time.Time
		for _, p := range pending {
			if first.IsZero() || p.fireAt.Before(first) {
				first = p.fireAt
			}
		}
		if first.IsZero() {
			return
		}
		timer = time.NewTimer(max(0, first.Sub(schedNow())))
		timerC = timer.C
	}

	// pump hands queued groups to the workers. A full queue is not a reason to
	// throw work away: the item stays in queued and is offered again the moment
	// a worker frees a slot, so dispatch neither drops a room nor blocks behind
	// the network.
	pump := func() {
		for _, groupID := range sortedKeys(queued) {
			if busy[groupID] {
				continue
			}
			select {
			case k.deviceLookups <- *queued[groupID]:
				busy[groupID] = true
				delete(queued, groupID)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case t := <-k.triggers:
			// Device lookups need the network. Doing them here would stall
			// every other room behind them - a whole-house power restore is
			// dozens of rate-limited GETs.
			if t.kind == kindDevice {
				k.queueDeviceLookup(queued, t)
				pump()
				continue
			}
			if k.schedule(pending, t) {
				arm()
			}

		case res := <-k.results:
			if k.finish(pending, res) {
				arm()
			}

		case groupID := <-k.lookupsDone:
			delete(busy, groupID)
			pump()

		case <-timerC:
			stopTimer()
			k.drain(pending)
			arm()
		}
	}
}

// schedule folds one trigger into the pending set, reporting whether the timer
// needs rearming.
func (k *Keeper) schedule(pending map[string]*pendingRecall, t trigger) bool {
	if t.kind == kindActivity {
		// Activity extends a wait, never starts one. That asymmetry is what
		// keeps a recall's own echo out of here: by the time the bridge
		// reports the lights it just turned on, the entry that caused them is
		// long gone from pending, so there is nothing for the echo to extend
		// and it cannot conjure a new entry either.
		group, ok := k.reg.GroupForLight(t.id)
		if !ok {
			return false
		}
		p, waiting := pending[group.ID]
		return waiting && k.extend(p)
	}

	r, ok := k.resolve(t)
	if !ok {
		return false
	}
	if p, exists := pending[r.groupID]; exists {
		// The same event seen through another light in the room, or a retry
		// already counting down. Either way this is more activity in a group
		// we are already going to recall.
		//
		// The exemption from commit's "is anything still on" veto has to carry
		// across the fold, though. A power_restored lookup deliberately never
		// writes what it read back into the registry, so the registry has no
		// record of the lamp returning; folding that trigger into an entry
		// created by an ordinary switch-on would leave the older reason in
		// place and the restore would be vetoed and dropped, with no retry.
		if r.reason == reasonPowerRestored {
			p.foldedPowerRestore = true
		}
		return k.extend(p)
	}

	k.mu.RLock()
	window, hard := k.cfg.CoalesceWindow.Duration(), k.cfg.CoalesceMax.Duration()
	k.mu.RUnlock()

	now := schedNow()
	k.seq++
	p := &pendingRecall{recall: r, seq: k.seq, hardAt: now.Add(hard)}
	p.fireAt = earlier(now.Add(window), p.hardAt)
	pending[r.groupID] = p
	return true
}

// extend restarts a pending group's wait, reporting whether anything moved.
//
// This is what makes coalesce_window an idle gap rather than a fixed window.
// A single switch flip sees no further activity and is acted on one gap later,
// exactly as before. A home automation writing to the whole house keeps
// resetting the gap, so the recall lands after it has finished rather than in
// the middle of it - which matters because the commands that would land on top
// of our scene arrive at lights that are already on, and so carry no off->on
// edge to trigger a second recall afterwards.
//
// hardAt is the escape hatch. A light that chatters - a dynamic scene, a lamp
// with a failing radio - would otherwise defer its group's recall forever.
func (k *Keeper) extend(p *pendingRecall) bool {
	next := earlier(schedNow().Add(k.coalesceWindow()), p.hardAt)
	if !next.After(p.fireAt) {
		// Never pull a deadline in. This is what stops a fresh trigger
		// cutting short the backoff of a retry waiting in the same slot.
		return false
	}
	p.fireAt = next
	return true
}

// drain commits every entry whose wait is over, oldest group first.
func (k *Keeper) drain(pending map[string]*pendingRecall) {
	now := schedNow()
	ready := make([]*pendingRecall, 0, len(pending))
	for _, p := range pending {
		if !p.fireAt.After(now) {
			ready = append(ready, p)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].seq < ready[j].seq })

	for _, p := range ready {
		if k.inFlight[p.groupID] {
			// The previous recall for this group is still on the wire. Wait
			// for its outcome rather than stacking a second one behind it,
			// which would leave two entries fighting over one group's
			// suppression state.
			p.fireAt = schedNow().Add(k.coalesceWindow())
			p.hardAt = later(p.hardAt, p.fireAt)
			continue
		}
		// Out of pending before the request goes anywhere: from here the
		// entry belongs to the sender, and the echo of the recall it is about
		// to cause must not find it.
		delete(pending, p.groupID)
		k.commit(p)
	}
}

// schedNow is the clock the debounce and the retry backoff run on.
//
// It is deliberately real time rather than the k.now test hook. Every deadline
// it produces is waited out by a real time.Timer, so the two have to agree: a
// test that freezes k.now to step over the ten-second recall floor without
// sleeping would otherwise strand every pending entry behind a deadline the
// timer reaches but the comparison never does.
//
// k.now stays the clock for policy - the recall floor and the suppression
// window - which is exactly the part a test needs to move by hand.
func schedNow() time.Time { return time.Now() }

// earlier and later order two instants. The min and max builtins do not take
// a time.Time, which is not an ordered type.
func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// sortedKeys gives the maps dispatch iterates a stable order, so a burst
// produces the same sequence of requests every time.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// queueDeviceLookup files a rejoined device's lights under the group each
// belongs to, merging into whatever is already waiting for that group. Called
// on the dispatch goroutine, which owns queued.
//
// Exclusions are applied here so that no rate-limiter token is ever spent on a
// light or a room the user asked us to leave alone.
func (k *Keeper) queueDeviceLookup(queued map[string]*deviceLookup, t trigger) {
	k.mu.RLock()
	excl := k.excl
	k.mu.RUnlock()

	for _, lightID := range k.reg.LightIDsForDevice(t.id) {
		if excl.LightExcluded(lightID) {
			continue
		}
		group, ok := k.reg.GroupForLight(lightID)
		if !ok || excl.GroupExcluded(group.ID) {
			continue
		}
		item := queued[group.ID]
		if item == nil {
			item = &deviceLookup{groupID: group.ID, reason: t.reason}
			queued[group.ID] = item
		}
		if !slices.Contains(item.lightIDs, lightID) {
			item.lightIDs = append(item.lightIDs, lightID)
		}
	}
}

// lookupWorker drains the power-restore lookup queue. Several run at once so a
// slow bridge cannot serialise a whole-house restore behind one room, and they
// run off the dispatch goroutine so those reads never stall an unrelated room's
// recall.
func (k *Keeper) lookupWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-k.deviceLookups:
			k.runLookup(ctx, item)
			// Hand the group back so dispatch stops coalescing onto it. The
			// ctx case is what it looks like here: this send can genuinely
			// block, and on shutdown dispatch is already gone.
			select {
			case k.lookupsDone <- item.groupID:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runLookup asks the bridge which of a group's rejoined lights actually came
// back on, and re-queues the first one it finds as an ordinary light trigger.
// One is enough: the group is recalled as a whole, so the remaining reads would
// only spend rate-limiter tokens to reach the same conclusion.
//
// It never writes the registry: a reply that arrives after the user has
// switched the lamp off again would otherwise overwrite newer state from the
// event stream.
func (k *Keeper) runLookup(ctx context.Context, item deviceLookup) {
	for _, id := range item.lightIDs {
		// Stop at the first sign of shutdown rather than walking the rest of
		// the group. The remaining reads cost little by then - the client's
		// rate limiter refuses a request on a dead context before it reserves
		// a slot or opens a connection - but a worker has no business still
		// working through a list on behalf of a dispatch goroutine that has
		// gone, and this is the only place that says so.
		if ctx.Err() != nil {
			return
		}
		light, err := k.client.GetLight(ctx, id)
		if err != nil {
			if ctx.Err() == nil {
				k.log.Warn("could not read light after power restore", "light", id, "err", err)
			}
			continue
		}
		if light.On != nil && light.On.On {
			k.emit(trigger{kind: kindLight, id: id, reason: item.reason})
			return
		}
	}
}

func (k *Keeper) coalesceWindow() time.Duration {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.cfg.CoalesceWindow.Duration()
}

// resolve turns a light trigger into zero or one recall, applying the
// exclusion rules and the post-recall suppression window.
func (k *Keeper) resolve(t trigger) (recall, bool) {
	k.mu.RLock()
	excl, cfg := k.excl, k.cfg
	k.mu.RUnlock()

	lightID := t.id
	light, _ := k.reg.Light(lightID)
	lightName := light.Name()
	if lightName == "" {
		lightName = lightID
	}

	if excl.LightExcluded(lightID) {
		k.log.Info("ignoring excluded light", "light", lightName, "reason", t.reason)
		return recall{}, false
	}
	group, ok := k.reg.GroupForLight(lightID)
	if !ok {
		k.log.Warn("light belongs to no room or zone; nothing to recall", "light", lightName)
		return recall{}, false
	}
	if excl.GroupExcluded(group.ID) {
		k.log.Info("ignoring light in excluded group",
			"light", lightName, "group", group.Name(), "reason", t.reason)
		return recall{}, false
	}
	// The suppression window: this is almost certainly the echo of our own
	// recall turning the room's lights on.
	if until, ok := k.suppressUntil[group.ID]; ok && k.now().Before(until) {
		k.log.Debug("suppressed trigger during recall cooldown",
			"light", lightName, "group", group.Name())
		return recall{}, false
	}

	scene, why, ok := k.smartSceneFor(cfg, group)
	if !ok {
		if !k.warnedNoScene[group.ID] {
			k.warnedNoScene[group.ID] = true
			k.log.Warn("cannot recall group", "group", group.Name(), "reason", why)
		}
		return recall{}, false
	}
	return recall{
		groupID:   group.ID,
		groupName: group.Name(),
		sceneID:   scene.ID,
		sceneName: scene.Name(),
		lightName: lightName,
		reason:    t.reason,
	}, true
}

// smartSceneFor picks the smart scene governing a group, preferring an
// explicit config override over name matching. On failure it returns a reason
// fit to show a user.
func (k *Keeper) smartSceneFor(cfg *config.Config, group hue.Group) (hue.SmartScene, string, bool) {
	if id, ok := cfg.SmartSceneOverride(group); ok {
		scene, found := k.reg.SmartScene(id)
		if !found {
			return hue.SmartScene{}, fmt.Sprintf(
				"smart_scene_overrides names smart scene %q, which does not exist on this bridge", id), false
		}
		if scene.Group.RID != group.ID {
			// Recalling it would restyle a different room entirely.
			return hue.SmartScene{}, fmt.Sprintf(
				"smart_scene_overrides names smart scene %q, which belongs to another group (%s)",
				id, scene.Group.RID), false
		}
		return scene, "", true
	}
	scene, ok := k.reg.SmartSceneForGroup(group.ID, cfg.SceneName)
	if !ok {
		return hue.SmartScene{}, fmt.Sprintf(
			"no smart scene named %q on this group; create it in the Hue app", cfg.SceneName), false
	}
	return scene, "", true
}

// commit enforces the minimum interval, arms the suppression window and hands
// the recall to the sender. It reports whether the recall was sent.
//
// Everything here happens on the dispatch goroutine. Only the HTTP round trip
// itself is elsewhere, so a slow or wedged bridge delays one room rather than
// freezing the scheduling of every other room behind it.
func (k *Keeper) commit(p *pendingRecall) bool {
	r := p.recall
	k.mu.RLock()
	minInterval := k.cfg.MinRecallInterval.Duration()
	cooldown := k.cfg.RecallCooldown.Duration()
	k.mu.RUnlock()

	now := k.now()
	if last, ok := k.lastRecall[r.groupID]; ok && now.Sub(last) < minInterval {
		k.log.Info("skipping recall, group recalled too recently",
			"group", r.groupName, "since", now.Sub(last).Round(time.Millisecond), "min", minInterval)
		return false
	}

	// A light switched on and straight back off inside the coalescing window
	// should not light the whole room.
	//
	// Only power_restored is exempt, and for one specific reason: its lookup
	// reads the lamp from the bridge and deliberately never writes what it
	// finds back into the registry, so the registry has no record of the lamp
	// returning and would veto every restore. Switch-on, startup and reconnect
	// all take their triggers from the registry in the first place, so asking
	// it again here is both meaningful and current.
	if !p.skipsOnCheck() && !k.reg.GroupHasLightOn(r.groupID) {
		k.log.Info("skipping recall, no light in the group is on any more", "group", r.groupName)
		return false
	}

	// Arm suppression *before* the request. The bridge starts turning lights
	// on the moment it accepts the recall, and those on-events would
	// otherwise come straight back at us as fresh triggers.
	p.prevRecall, p.hadRecall = k.lastRecall[r.groupID]
	p.armedUntil = now.Add(cooldown)
	p.prevSuppress = k.armSuppression(r.groupID, p.armedUntil)
	k.lastRecall[r.groupID] = now

	select {
	case k.sendQ <- p:
		k.inFlight[r.groupID] = true
		return true
	default:
		// Deeper than the bridge has rooms, so this means the sender has
		// stopped draining it entirely.
		k.log.Error("recall queue full, dropping",
			"group", r.groupName, "scene", r.sceneName)
		k.rollback(p)
		return false
	}
}

// armSuppression opens the post-recall window on every group this recall's echo
// can be attributed to, returning what each of them held so rollback can put it
// back.
//
// The recalled group is not always the only one. The bridge lights every light
// in the group and reports each as newly on, and those events are resolved back
// through GroupForLight, which prefers a light's room over any zone it is in.
// For a room recall that lands on the room itself and the set is a singleton.
// For a zone recall it does not: a zone only wins for a light that is in no room
// at all, so a zone mixing such a light with room-owning ones sends its echo
// into those rooms - where, with no window of their own, every one of them
// scheduled a recall of its own off the back of ours.
//
// lastRecall is deliberately not touched for the extra groups. It is the floor
// on how often a room may be restyled, not an echo filter, and a room must not
// lose its next ten seconds because a zone it lends a light to was recalled.
func (k *Keeper) armSuppression(groupID string, until time.Time) []groupSuppression {
	groups := []string{groupID}
	for _, lightID := range k.reg.LightIDsInGroup(groupID) {
		g, ok := k.reg.GroupForLight(lightID)
		if !ok || slices.Contains(groups, g.ID) {
			continue
		}
		groups = append(groups, g.ID)
	}

	prev := make([]groupSuppression, 0, len(groups))
	for _, id := range groups {
		was, had := k.suppressUntil[id]
		prev = append(prev, groupSuppression{groupID: id, until: was, had: had})
		k.suppressUntil[id] = until
	}
	return prev
}

// rollback undoes what commit armed, over exactly the set it armed.
//
// The bridge did not accept the recall, so no echo is coming and nothing must
// be suppressed. Leaving the window armed would lock the room out for the whole
// min-recall interval over a transient error - and would block the retry too.
func (k *Keeper) rollback(p *pendingRecall) {
	if p.hadRecall {
		k.lastRecall[p.groupID] = p.prevRecall
	} else {
		delete(k.lastRecall, p.groupID)
	}
	for _, s := range p.prevSuppress {
		if cur, ok := k.suppressUntil[s.groupID]; ok && !cur.Equal(p.armedUntil) {
			// Something newer armed this group after we did. inFlight rules
			// that out for the recalled group, but not for the rooms a zone
			// recall reaches: those are reachable from several groups at once,
			// and putting our older value back would strip a window that is
			// still holding an echo off.
			continue
		}
		if s.had {
			k.suppressUntil[s.groupID] = s.until
		} else {
			delete(k.suppressUntil, s.groupID)
		}
	}
}

// sender performs the recall requests, one at a time.
//
// One sender is enough: the client's rate limiter spaces requests out anyway,
// so a second would only queue behind the first, and one keeps recalls going
// out in the order dispatch decided on.
func (k *Keeper) sender(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-k.sendQ:
			var err error
			if !k.dryRun {
				err = k.client.RecallSmartScene(ctx, p.sceneID)
			}
			select {
			case k.results <- outcome{pending: p, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// finish records what the bridge said, retrying if it is worth retrying. It
// reports whether the timer needs rearming.
func (k *Keeper) finish(pending map[string]*pendingRecall, res outcome) bool {
	p := res.pending
	delete(k.inFlight, p.groupID)

	if res.err == nil {
		msg := "recalled smart scene"
		if k.dryRun {
			msg = "would recall smart scene (dry run)"
		}
		k.log.Info(msg, "group", p.groupName, "scene", p.sceneName,
			"trigger", p.lightName, "reason", p.reason)
		if k.onRecalled != nil {
			k.onRecalled(p.recall)
		}
		return false
	}

	// Unconditionally, including on a timeout, where the bridge may in fact
	// have applied the recall. Keeping the window armed there would be the
	// safer-looking choice, but the floor it also keeps armed blocks this
	// recall's own retry, so a wedged bridge would cost the room its recovery
	// as well as its recall. The echo such a recall produces is covered by the
	// cooldown, which is shorter than the request timeout that got us here.
	k.rollback(p)

	attempts := p.attempt + 1
	switch {
	case !hue.Retryable(res.err):
		// A deleted scene or a revoked app key needs a human. Repeating it
		// would only add load to a bridge that has already given its answer.
		k.log.Error("recall failed", "group", p.groupName, "scene", p.sceneName,
			"attempts", attempts, "err", res.err)
		return false
	case attempts > len(recallBackoff):
		k.log.Error("recall failed, giving up", "group", p.groupName,
			"scene", p.sceneName, "attempts", attempts, "err", res.err)
		return false
	}

	if _, exists := pending[p.groupID]; exists {
		// Something newer is already waiting for this group. It carries a
		// fresher reason and trigger, so let it have the slot.
		k.log.Warn("recall failed, superseded by a newer trigger",
			"group", p.groupName, "err", res.err)
		return false
	}

	backoff := recallBackoff[p.attempt]
	p.attempt = attempts
	p.fireAt = schedNow().Add(backoff)
	// A retry is not up for further debouncing: it has already waited out a
	// gap once, and hardAt == fireAt makes extend() a no-op for it.
	p.hardAt = p.fireAt
	pending[p.groupID] = p
	k.log.Warn("recall failed, retrying", "group", p.groupName, "scene", p.sceneName,
		"attempt", attempts, "in", backoff, "err", res.err)
	return true
}

// Explain describes what the daemon would do for a light, without touching it.
// It powers the `resolve` command, which is what a user reaches for when the
// daemon is not behaving - so it must not overstate what would happen.
func (k *Keeper) Explain(query string) string {
	k.mu.RLock()
	excl, cfg := k.excl, k.cfg
	k.mu.RUnlock()

	want := strings.ToLower(strings.TrimSpace(query))
	var match *hue.Light
	for _, l := range k.reg.Lights() {
		if strings.ToLower(l.ID) == want || strings.ToLower(l.Name()) == want {
			light := l
			match = &light
			break
		}
	}
	if match == nil {
		return fmt.Sprintf("no light matches %q (try `hue-scene-keeper list`)", query)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "light:  %s (%s)\n", match.Name(), match.ID)
	on, onKnown := k.reg.LightIsOn(match.ID)
	switch {
	case !onKnown:
		b.WriteString("state:  unknown\n")
	case on:
		b.WriteString("state:  on\n")
	default:
		b.WriteString("state:  off\n")
	}

	if excl.LightExcluded(match.ID) {
		b.WriteString("result: SKIPPED - light is excluded, it never triggers a recall\n")
		b.WriteString("        (it will still be lit by a recall another light in its room causes)\n")
		return b.String()
	}
	group, ok := k.reg.GroupForLight(match.ID)
	if !ok {
		b.WriteString("result: SKIPPED - light belongs to no room or zone\n")
		return b.String()
	}
	fmt.Fprintf(&b, "group:  %s (%s, %s)\n", group.Name(), group.Type, group.ID)
	if excl.GroupExcluded(group.ID) {
		b.WriteString("result: SKIPPED - group is excluded, it is never recalled\n")
		return b.String()
	}
	scene, why, ok := k.smartSceneFor(cfg, group)
	if !ok {
		fmt.Fprintf(&b, "result: SKIPPED - %s\n", why)
		return b.String()
	}
	fmt.Fprintf(&b, "scene:  %s (%s, currently %s)\n", scene.Name(), scene.ID, orUnknown(scene.State))

	if onKnown && on {
		// Being honest here matters: a switched-on light is the single most
		// likely thing a puzzled user will ask about.
		b.WriteString("result: NOTHING right now - this light is already on, and only an\n")
		b.WriteString("        off-to-on transition triggers a recall. Switch it off and on\n")
		b.WriteString("        again to trigger one. A mains cut only triggers a recall if\n")
		b.WriteString("        it lasted long enough for the bridge to mark the lamp\n")
		b.WriteString("        unreachable, which takes about a minute; a shorter cut\n")
		b.WriteString("        produces no event at all.\n")
	} else {
		fmt.Fprintf(&b, "result: switching this light on would PUT recall:activate on smart_scene/%s\n", scene.ID)
		fmt.Fprintf(&b, "        turning on all %d lights in %s\n",
			len(k.reg.LightIDsInGroup(group.ID)), group.Name())
	}
	fmt.Fprintf(&b, "limits: the recall waits for %s of quiet in this group, deferred\n",
		cfg.CoalesceWindow.Duration())
	fmt.Fprintf(&b, "        by further activity there but never past %s. Afterwards\n",
		cfg.CoalesceMax.Duration())
	fmt.Fprintf(&b, "        triggers are suppressed %s, and this group gets\n", cfg.RecallCooldown.Duration())
	fmt.Fprintf(&b, "        at most one recall per %s\n", cfg.MinRecallInterval.Duration())
	return b.String()
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
