package keeper

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/config"
	"github.com/csstaub/hue-scene-keeper/internal/hue"
	"github.com/csstaub/hue-scene-keeper/internal/hue/fake"
	"github.com/csstaub/hue-scene-keeper/internal/registry"
)

const (
	kitchenScene = "scene-kitchen"
	bedroomScene = "scene-bedroom"
	zoneScene    = "scene-downstairs"
)

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.CoalesceWindow = config.Duration(100 * time.Millisecond)
	cfg.RecallCooldown = config.Duration(2 * time.Second)
	return cfg
}

func testLogger(t *testing.T) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// kitchenBridge builds a bridge with one four-light kitchen that has a Natural
// Light smart scene.
func kitchenBridge(t *testing.T) (*fake.Bridge, []string) {
	t.Helper()
	b := fake.NewBridge(t)
	lights := b.AddRoom("room-kitchen", "Kitchen", "Ceiling", "Counter", "Island", "Pantry")
	b.AddSmartScene(kitchenScene, "Natural Light", "room-kitchen", hue.TypeRoom)
	return b, lights
}

func startKeeper(t *testing.T, b *fake.Bridge, cfg *config.Config) (*Keeper, *registry.Registry) {
	t.Helper()
	if cfg == nil {
		cfg = testConfig()
	}
	reg := registry.New()
	k := New(b.Client(), reg, cfg, testLogger(t), false)
	startPreparedKeeper(t, b, k, reg)
	return k, reg
}

// startPreparedKeeper runs an already-constructed keeper, for tests that must
// install a hook such as the clock before the dispatch goroutine exists and
// could race with the assignment.
func startPreparedKeeper(t *testing.T, b *fake.Bridge, k *Keeper, reg *registry.Registry) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = k.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("keeper did not shut down")
		}
	})

	if !b.WaitForSubscriber(5 * time.Second) {
		t.Fatal("keeper never connected to the event stream")
	}
	waitFor(t, 5*time.Second, "initial sync", func() bool {
		lights, rooms, _, scenes := reg.Counts()
		return lights > 0 && rooms > 0 && scenes > 0
	})
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitForRecalls(t *testing.T, b *fake.Bridge, n int) {
	t.Helper()
	waitFor(t, 5*time.Second, "recalls", func() bool { return len(b.Recalls()) >= n })
}

// settle gives any further (unwanted) recalls time to appear.
func settle() { time.Sleep(600 * time.Millisecond) }

// commitRecall drives one recall through the admission checks and, if they
// pass, sends it the way the sender goroutine would.
//
// Recalls are dispatched asynchronously now, so a test that wants to assert on
// what reached the bridge immediately afterwards has to close that loop
// itself. It reports whether the recall was admitted.
func commitRecall(t *testing.T, ctx context.Context, k *Keeper, r recall) bool {
	t.Helper()
	p := &pendingRecall{recall: r}
	if !k.commit(p) {
		return false
	}
	<-k.sendQ
	err := k.client.RecallSmartScene(ctx, p.sceneID)
	k.finish(map[string]*pendingRecall{}, outcome{pending: p, err: err})
	return err == nil
}

func TestRecallsRoomWhenLightSwitchesOn(t *testing.T) {
	b, lights := kitchenBridge(t)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	waitForRecalls(t, b, 1)

	if got := b.Recalls(); len(got) != 1 || got[0] != kitchenScene {
		t.Fatalf("expected one recall of %s, got %v", kitchenScene, got)
	}
}

// TestDoesNotLoopWhenRecallTurnsOnWholeRoom is the most important test here.
//
// Recalling a room turns on every light in it, and the bridge reports each of
// those as newly on. Without room-scoped suppression every one of those echoes
// is a fresh off->on trigger and the daemon recalls forever.
func TestDoesNotLoopWhenRecallTurnsOnWholeRoom(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.EchoOnRecall = true
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	waitForRecalls(t, b, 1)

	// Three more lights just reported themselves on. None may cause a recall.
	settle()
	settle()

	if got := b.Recalls(); len(got) != 1 {
		t.Fatalf("feedback loop: expected exactly 1 recall, got %d (%v)", len(got), got)
	}
}

func TestAlreadyOnLightDoesNotTrigger(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)
	startKeeper(t, b, nil)

	// The bridge re-reports a light that was already on, e.g. a brightness
	// change. There is no off->on edge, so nothing should happen.
	b.SwitchLight(lights[0], true)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("expected no recall for an already-on light, got %v", got)
	}
}

func TestExcludedLightDoesNotTriggerButRoomStillWorks(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.Exclude.Lights = []string{"Pantry"}
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[3], true) // Pantry
	settle()
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("excluded light triggered a recall: %v", got)
	}

	// A normal light in the same room must still work.
	b.SwitchLight(lights[0], true)
	waitForRecalls(t, b, 1)
}

func TestExcludedRoomIsNeverRecalled(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.Exclude.Rooms = []string{"kitchen"} // case-insensitive
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("excluded room was recalled: %v", got)
	}
}

// TestPowerRestoreRecalls covers the case an off->on edge cannot see: the lamp
// was on before its mains was cut, and is on again when it returns.
func TestPowerRestoreRecalls(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)
	startKeeper(t, b, nil)

	connID := b.ConnectivityIDFor(lights[0])
	if connID == "" {
		t.Fatal("no connectivity service for the light")
	}
	b.SetConnectivity(connID, hue.StatusConnectivityIssue)
	settle()
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("losing power should not recall anything, got %v", got)
	}

	b.SetConnectivity(connID, hue.StatusConnected)
	waitForRecalls(t, b, 1)
}

func TestPowerRestoreOfAnOffLightDoesNothing(t *testing.T) {
	b, lights := kitchenBridge(t)
	startKeeper(t, b, nil)

	connID := b.ConnectivityIDFor(lights[0])
	b.SetConnectivity(connID, hue.StatusConnectivityIssue)
	b.SetConnectivity(connID, hue.StatusConnected)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("a lamp that came back off should not recall, got %v", got)
	}
}

func TestBurstCoalescesToOneRecall(t *testing.T) {
	b, lights := kitchenBridge(t)
	startKeeper(t, b, nil)

	for _, id := range lights {
		b.SwitchLight(id, true)
	}
	waitForRecalls(t, b, 1)
	settle()

	if got := b.Recalls(); len(got) != 1 {
		t.Fatalf("expected a burst to coalesce into 1 recall, got %d (%v)", len(got), got)
	}
}

func TestRoomWithoutSmartSceneIsSkipped(t *testing.T) {
	b := fake.NewBridge(t)
	lights := b.AddRoom("room-hall", "Hall", "Hall Light")
	// Deliberately no smart scene for this room.
	startKeeperNoScene(t, b)

	b.SwitchLight(lights[0], true)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("expected no recall without a smart scene, got %v", got)
	}
}

// startKeeperNoScene starts a keeper on a bridge that has no smart scenes,
// where the usual readiness check would never pass.
func startKeeperNoScene(t *testing.T, b *fake.Bridge) {
	t.Helper()
	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = k.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	if !b.WaitForSubscriber(5 * time.Second) {
		t.Fatal("keeper never connected")
	}
	waitFor(t, 5*time.Second, "initial sync", func() bool {
		lights, rooms, _, _ := reg.Counts()
		return lights > 0 && rooms > 0
	})
}

func TestZoneIsUsedWhenLightHasNoRoom(t *testing.T) {
	b := fake.NewBridge(t)
	// A room is needed to create devices and lights, but the zone is what
	// carries the smart scene here.
	lights := b.AddRoom("room-none", "Unassigned", "Lamp A", "Lamp B")
	b.AddZone("zone-desk", "Desk", lights...)
	b.AddSmartScene("scene-desk", "Natural Light", "zone-desk", hue.TypeZone)

	_, reg := startKeeperZone(t, b)

	// The light is in a room, so the room wins - and that room has no scene.
	if group, ok := reg.GroupForLight(lights[0]); !ok || group.ID != "room-none" {
		t.Fatalf("expected the room to win over the zone, got %+v", group)
	}

	b.SwitchLight(lights[0], true)
	settle()
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("room without a scene should not fall through to the zone, got %v", got)
	}
}

func startKeeperZone(t *testing.T, b *fake.Bridge) (*Keeper, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = k.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	if !b.WaitForSubscriber(5 * time.Second) {
		t.Fatal("keeper never connected")
	}
	waitFor(t, 5*time.Second, "initial sync", func() bool {
		lights, _, zones, scenes := reg.Counts()
		return lights > 0 && zones > 0 && scenes > 0
	})
	return k, reg
}

// TestMinRecallIntervalBlocksRapidRepeat checks the hard floor that backstops
// the cooldown. The clock is driven manually so the test does not sleep.
func TestMinRecallIntervalBlocksRapidRepeat(t *testing.T) {
	b, lights := kitchenBridge(t)
	// The room needs a light on, or the "nothing is on any more" guard vetoes
	// every recall before the interval is ever consulted.
	b.SetLightStateSilently(lights[0], true)
	cfg := testConfig()
	reg := registry.New()
	k := New(b.Client(), reg, cfg, testLogger(t), false)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	now := time.Now()
	k.now = func() time.Time { return now }
	r := recall{
		groupID: "room-kitchen", groupName: "Kitchen",
		sceneID: kitchenScene, sceneName: "Natural Light", reason: reasonSwitchedOn,
	}

	commitRecall(t, ctx, k, r)
	if got := len(b.Recalls()); got != 1 {
		t.Fatalf("first recall should go through, got %d", got)
	}

	// Well inside the 10s floor.
	now = now.Add(3 * time.Second)
	commitRecall(t, ctx, k, r)
	if got := len(b.Recalls()); got != 1 {
		t.Fatalf("second recall inside the floor should be blocked, got %d", got)
	}

	// Past it.
	now = now.Add(config.MinRecallFloor)
	commitRecall(t, ctx, k, r)
	if got := len(b.Recalls()); got != 2 {
		t.Fatalf("recall after the floor should go through, got %d", got)
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	b, lights := kitchenBridge(t)
	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), true)

	// Set the hook before Run, so the dispatch goroutine never races with it.
	recalled := make(chan recall, 4)
	k.onRecalled = func(r recall) { recalled <- r }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = k.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	if !b.WaitForSubscriber(5 * time.Second) {
		t.Fatal("keeper never connected")
	}
	waitFor(t, 5*time.Second, "initial sync", func() bool {
		l, _, _, s := reg.Counts()
		return l > 0 && s > 0
	})

	b.SwitchLight(lights[0], true)
	select {
	case r := <-recalled:
		if r.sceneID != kitchenScene {
			t.Fatalf("unexpected scene %s", r.sceneID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dry run never resolved a recall")
	}
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("dry run sent %d recalls to the bridge", len(got))
	}
}

// --- regression tests for the adversarial review findings -------------------

// TestFailedRecallDoesNotLockOutTheRoom: commit arms the cooldown before
// sending. If a transient bridge error left that window armed, one 503 would
// silence the room for the whole min-recall interval, and the user would just
// find their lights unchanged. The retry makes that harder to notice, so this
// still checks the rollback rather than only the recovery.
func TestFailedRecallDoesNotLockOutTheRoom(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.FailNextRecalls(1)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	settle()
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("the first recall was supposed to fail, got %v", got)
	}

	// The bridge has recovered; another light coming on must work, well
	// inside the 10s min_recall_interval that a stuck lastRecall would impose.
	b.SwitchLight(lights[1], true)
	waitForRecalls(t, b, 1)
}

// TestStartupSkipsExcludedLightButStillRecallsTheRoom: a group gets one startup
// trigger. Spending it on an excluded light silently dropped the whole room.
// "Ceiling" sorts before "Pantry" in the id ordering used to pick the light.
func TestStartupSkipsExcludedLightButStillRecallsTheRoom(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true) // Ceiling, excluded, sorts first
	b.SetLightStateSilently(lights[1], true) // Counter, not excluded

	cfg := testConfig()
	cfg.ApplyOnStartup = true
	cfg.Exclude.Lights = []string{"Ceiling"}
	startKeeper(t, b, cfg)

	waitForRecalls(t, b, 1)
}

// TestLightThatCameOnWhileDisconnectedIsRecalled: the resync records it as
// already on, so there is no off->on edge and it would otherwise be missed
// entirely. This is the half of the reconnect path that used to be silent.
func TestLightThatCameOnWhileDisconnectedIsRecalled(t *testing.T) {
	b, lights := kitchenBridge(t)
	startKeeper(t, b, nil)

	b.DropStreams()
	waitFor(t, 5*time.Second, "the stream to drop", func() bool { return b.SubscriberCount() == 0 })

	// Happens while we are blind: no event is published.
	b.SetLightStateSilently(lights[0], true)

	waitFor(t, 15*time.Second, "reconnect", func() bool { return b.SubscriberCount() > 0 })
	waitForRecalls(t, b, 1)
}

// TestLightAlreadyOnBeforeDisconnectIsNotRecalled is the other half: a
// reconnect must not restyle rooms that were simply left on.
func TestLightAlreadyOnBeforeDisconnectIsNotRecalled(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)
	startKeeper(t, b, nil)

	b.DropStreams()
	waitFor(t, 5*time.Second, "the stream to drop", func() bool { return b.SubscriberCount() == 0 })
	waitFor(t, 15*time.Second, "reconnect", func() bool { return b.SubscriberCount() > 0 })
	settle()
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("a reconnect must not recall rooms that were already on, got %v", got)
	}
}

// TestLightSwitchedStraightBackOffDoesNotRecall: a fumbled switch inside the
// coalescing window should not light the whole room.
func TestLightSwitchedStraightBackOffDoesNotRecall(t *testing.T) {
	b, lights := kitchenBridge(t)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	b.SwitchLight(lights[0], false)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("expected no recall for a light already off again, got %v", got)
	}
}

// TestOverridePointingAtAnotherGroupsSceneIsRefused: recalling it would restyle
// a completely different room, which is worse than doing nothing.
func TestOverridePointingAtAnotherGroupsSceneIsRefused(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.AddRoom("room-den", "Den", "Den Lamp")
	b.AddSmartScene(bedroomScene, "Natural Light", "room-den", hue.TypeRoom)

	cfg := testConfig()
	cfg.SmartSceneOverrides = map[string]string{"Kitchen": bedroomScene}
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true)
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("an override naming another group's scene must be refused, got %v", got)
	}
}

// TestExplainDoesNotClaimAnAlreadyOnLightWouldRecall: `resolve` is the tool a
// puzzled user reaches for, so it must not promise a recall that Trigger A
// cannot produce.
func TestExplainDoesNotClaimAnAlreadyOnLightWouldRecall(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)

	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	out := k.Explain("Ceiling")
	if !strings.Contains(out, "already on") {
		t.Errorf("Explain should say an already-on light triggers nothing:\n%s", out)
	}
	if strings.Contains(out, "would PUT recall") {
		t.Errorf("Explain overstated what would happen:\n%s", out)
	}
	// The limits a user needs to know about when it "isn't working".
	for _, want := range []string{"of quiet", "never past", "suppressed", "at most one recall"} {
		if !strings.Contains(out, want) {
			t.Errorf("Explain omitted %q:\n%s", want, out)
		}
	}

	out = k.Explain("Counter") // off
	if !strings.Contains(out, "would PUT recall") {
		t.Errorf("Explain should report a recall for an off light:\n%s", out)
	}
}

// TestWholeHouseRestoreRecallsEveryRoom: a fuse or a breaker spanning several
// rooms is the whole reason Trigger B exists, and the lookup pool used to be a
// drop semaphore four deep - so eight rooms rejoining at once produced four
// recalls and four warnings, with the rest thrown away and never retried.
func TestWholeHouseRestoreRecallsEveryRoom(t *testing.T) {
	const rooms = 8

	b := fake.NewBridge(t)
	var connIDs []string
	for i := range rooms {
		roomID := fmt.Sprintf("room-%d", i)
		lights := b.AddRoom(roomID, fmt.Sprintf("Room %d", i), "Lamp")
		b.AddSmartScene("scene-"+roomID, "Natural Light", roomID, hue.TypeRoom)
		// On before the cut, and on again when the power returns: exactly the
		// case an off->on edge cannot see.
		b.SetLightStateSilently(lights[0], true)
		connIDs = append(connIDs, b.ConnectivityIDFor(lights[0]))
	}
	startKeeper(t, b, nil)

	for _, id := range connIDs {
		b.SetConnectivity(id, hue.StatusConnectivityIssue)
	}
	for _, id := range connIDs {
		b.SetConnectivity(id, hue.StatusConnected)
	}

	waitForRecalls(t, b, rooms)
	settle()
	if got := b.Recalls(); len(got) != rooms {
		t.Fatalf("expected all %d rooms recalled, got %d (%v)", rooms, len(got), got)
	}
}

// TestManyLampsInOneRoomRecallItOnce: the other half of the same fix. Every
// lamp in a room rejoining is one room's worth of work, and it must collapse to
// a single recall rather than one per lamp.
func TestManyLampsInOneRoomRecallItOnce(t *testing.T) {
	b, lights := kitchenBridge(t)
	for _, id := range lights {
		b.SetLightStateSilently(id, true)
	}
	startKeeper(t, b, nil)

	var connIDs []string
	for _, id := range lights {
		connIDs = append(connIDs, b.ConnectivityIDFor(id))
	}
	for _, id := range connIDs {
		b.SetConnectivity(id, hue.StatusConnectivityIssue)
	}
	for _, id := range connIDs {
		b.SetConnectivity(id, hue.StatusConnected)
	}

	waitForRecalls(t, b, 1)
	settle()
	if got := b.Recalls(); len(got) != 1 {
		t.Fatalf("expected one recall for one room, got %d (%v)", len(got), got)
	}
}

// TestASecondRestoreOfTheSameRoomStillWorks: coalescing by group is a property
// of one burst, not a permanent veto. If the group were never released, the
// first restore after startup would be the last one that room ever got.
func TestASecondRestoreOfTheSameRoomStillWorks(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)

	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)
	// The 10s recall floor is not configurable, so a second recall of the same
	// room only appears if the clock has moved past it. Installed before Run,
	// and read through an atomic, so nothing races with the dispatch goroutine.
	base := time.Now()
	var advance atomic.Int64
	k.now = func() time.Time { return base.Add(time.Duration(advance.Load())) }
	startPreparedKeeper(t, b, k, reg)

	connID := b.ConnectivityIDFor(lights[0])
	b.SetConnectivity(connID, hue.StatusConnectivityIssue)
	b.SetConnectivity(connID, hue.StatusConnected)
	waitForRecalls(t, b, 1)

	advance.Store(int64(2 * config.MinRecallFloor))
	b.SetConnectivity(connID, hue.StatusConnectivityIssue)
	b.SetConnectivity(connID, hue.StatusConnected)
	waitForRecalls(t, b, 2)
}

// TestConnectivityRestoredWhileDisconnectedIsRecalled: Trigger B fires from a
// stream event, so a mains restore landing while the stream is down used to be
// lost for good - the next resync recorded `connected` as though it had always
// been so. Trigger A has recovery for this shape of problem; this is Trigger B's.
func TestConnectivityRestoredWhileDisconnectedIsRecalled(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)
	_, reg := startKeeper(t, b, nil)

	connID := b.ConnectivityIDFor(lights[0])
	b.SetConnectivity(connID, hue.StatusConnectivityIssue)
	waitFor(t, 5*time.Second, "the lost radio to reach the registry", func() bool {
		c, ok := reg.Connectivity(connID)
		return ok && c.Status == hue.StatusConnectivityIssue
	})

	b.DropStreams()
	waitFor(t, 5*time.Second, "the stream to drop", func() bool { return b.SubscriberCount() == 0 })

	// The power comes back while we are blind: the bridge's own state moves,
	// but the event it publishes reaches nobody.
	b.SetConnectivity(connID, hue.StatusConnected)

	waitFor(t, 15*time.Second, "reconnect", func() bool { return b.SubscriberCount() > 0 })
	waitForRecalls(t, b, 1)
}

// TestReconnectRecallIsVetoedWhenNoLightIsOn: the reconnect and startup paths
// take their triggers from the registry, so the registry is exactly the right
// thing to ask again once the coalescing window closes. Only power_restored is
// exempt, because its lookup deliberately never writes what it read back.
func TestReconnectRecallIsVetoedWhenNoLightIsOn(t *testing.T) {
	b, _ := kitchenBridge(t) // every light off
	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	r := recall{groupID: "room-kitchen", groupName: "Kitchen", sceneID: kitchenScene, sceneName: "Natural Light"}
	for _, reason := range []string{reasonSwitchedOn, reasonReconnect, reasonStartup} {
		r.reason = reason
		commitRecall(t, ctx, k, r)
		if got := b.Recalls(); len(got) != 0 {
			t.Fatalf("%s: recalled a room with no light on: %v", reason, got)
		}
	}

	r.reason = reasonPowerRestored
	commitRecall(t, ctx, k, r)
	if got := b.Recalls(); len(got) != 1 {
		t.Fatalf("power_restored must keep its exemption, got %v", got)
	}
}

// TestExplainDoesNotPromiseAShortMainsCutTriggersARecall: measured on a real
// bridge, a ten-second cut leaves the lamp reported on and connected throughout,
// so `resolve` must not send a puzzled user off to test with one.
func TestExplainDoesNotPromiseAShortMainsCutTriggersARecall(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)

	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	out := k.Explain("Ceiling")
	if strings.Contains(out, "restore its power") {
		t.Errorf("Explain still offers a power cycle as a way to trigger a recall:\n%s", out)
	}
	for _, want := range []string{"mark the lamp", "about a minute", "no event at all"} {
		if !strings.Contains(out, want) {
			t.Errorf("Explain omitted the mains-cut caveat (%q):\n%s", want, out)
		}
	}
}

// --- the debounce ------------------------------------------------------------

// TestLoneSwitchIsStillActedOnPromptly guards the responsiveness the debounce
// was not allowed to cost. One light coming on sees no further activity, so its
// recall must land after one coalesce window, not after coalesce_max.
func TestLoneSwitchIsStillActedOnPromptly(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.CoalesceWindow = config.Duration(100 * time.Millisecond)
	cfg.CoalesceMax = config.Duration(3 * time.Second)
	startKeeper(t, b, cfg)

	start := time.Now()
	b.SwitchLight(lights[0], true)
	waitForRecalls(t, b, 1)

	// Generous against a loaded CI box, but nowhere near coalesce_max: the
	// failure this catches is the window being treated as a floor.
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a lone switch waited %s; it should go after one %s window",
			took, cfg.CoalesceWindow.Duration())
	}
}

// TestActivityInAPendingRoomDefersTheRecall is the whole point of the exercise.
//
// A home automation turning the house on writes to each light in turn. The
// first write is an off->on edge and schedules a recall; the rest land on
// lights the scene has already turned on, so they carry no edge and used to be
// invisible. Recalling in the middle of that leaves the room half in its scene
// and half in whatever the automation wrote, with nothing left to trigger a
// correction.
func TestActivityInAPendingRoomDefersTheRecall(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.CoalesceWindow = config.Duration(200 * time.Millisecond)
	cfg.CoalesceMax = config.Duration(5 * time.Second)
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true)

	// Keep writing to the room for well over a window's worth of time, the
	// way a slow automation would.
	for i := range 5 {
		time.Sleep(100 * time.Millisecond)
		b.TouchLight(lights[1], float64(20+10*i))
		if got := b.Recalls(); len(got) != 0 {
			t.Fatalf("recalled after %dms while the room was still being written to", (i+1)*100)
		}
	}

	// Quiet now: the recall must arrive.
	waitForRecalls(t, b, 1)
}

// TestActivityCannotDeferPastCoalesceMax: a light that chatters - a dynamic
// scene, a lamp with a failing radio - would otherwise defer its room forever.
func TestActivityCannotDeferPastCoalesceMax(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.CoalesceWindow = config.Duration(200 * time.Millisecond)
	cfg.CoalesceMax = config.Duration(600 * time.Millisecond)
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				b.TouchLight(lights[1], float64(1+i%100))
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })

	// Never quiet, so only the cap can release it.
	waitForRecalls(t, b, 1)
}

// TestExcludedLightActivityDoesNotDeferTheRoom: an excluded light does not
// trigger a recall, and does not delay one either. The light people exclude is
// typically the one that never sits still, so leaving it able to extend the
// wait held its room at coalesce_max for as long as it chattered - the setting
// offered to silence it left it setting the pace instead.
func TestExcludedLightActivityDoesNotDeferTheRoom(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	cfg.CoalesceWindow = config.Duration(200 * time.Millisecond)
	cfg.CoalesceMax = config.Duration(5 * time.Second)
	cfg.Exclude.Lights = []string{"Pantry"}
	startKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true) // Ceiling: a real trigger, room now pending

	// The excluded light chatters throughout, faster than the window. Without
	// the exclusion check each touch restarts the wait and nothing is recalled
	// until coalesce_max releases it, five seconds away.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-ticker.C:
				b.TouchLight(lights[3], float64(1+i%100)) // Pantry
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })

	// One window is 200ms; allow generously for scheduling, but stay far below
	// the 5s cap so a pass cannot be the cap letting it through.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(b.Recalls()) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("excluded light's chatter deferred the room past 2s; recalls=%v", b.Recalls())
}

// TestActivityAloneNeverRecalls: activity extends a wait, it does not start
// one. Without that asymmetry every brightness change in the house would
// recall its room, and a recall's own echo would feed itself.
func TestActivityAloneNeverRecalls(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.SetLightStateSilently(lights[0], true)
	startKeeper(t, b, nil)

	for i := range 5 {
		b.TouchLight(lights[0], float64(10+10*i))
	}
	settle()

	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("activity on its own caused %d recalls: %v", len(got), got)
	}
}

// TestWholeHouseBurstRecallsEachRoomExactlyOnce is the shape of a GPS arrival
// automation: every room lit at once, from a cold start with everything off.
func TestWholeHouseBurstRecallsEachRoomExactlyOnce(t *testing.T) {
	b := fake.NewBridge(t)
	rooms := []string{"Kitchen", "Bedroom", "Hall", "Study"}
	var all [][]string
	for i, name := range rooms {
		id := fmt.Sprintf("room-%d", i)
		all = append(all, b.AddRoom(id, name, name+" A", name+" B", name+" C"))
		b.AddSmartScene(fmt.Sprintf("scene-%d", i), "Natural Light", id, hue.TypeRoom)
	}
	startKeeper(t, b, nil)

	for _, lights := range all {
		for _, id := range lights {
			b.SwitchLight(id, true)
		}
	}

	waitForRecalls(t, b, len(rooms))
	settle()

	got := b.Recalls()
	if len(got) != len(rooms) {
		t.Fatalf("expected one recall per room (%d), got %d: %v", len(rooms), len(got), got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("room %s was recalled twice: %v", id, got)
		}
		seen[id] = true
	}
}

// --- retries -----------------------------------------------------------------

// TestRateLimitedRecallIsRetried: a whole-house arrival is exactly when the
// bridge starts refusing requests, and it is the worst time to drop a room on
// the floor with no retry.
func TestRateLimitedRecallIsRetried(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.FailNextRecallsWith(1, http.StatusTooManyRequests)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	waitForRecalls(t, b, 1)

	if got := b.Recalls(); len(got) != 1 || got[0] != kitchenScene {
		t.Fatalf("expected the retry to recall %s, got %v", kitchenScene, got)
	}
}

// TestPermanentFailureIsNotRetried: a deleted scene or a revoked app key needs
// a human. Repeating it only adds load to a bridge that has already answered.
func TestPermanentFailureIsNotRetried(t *testing.T) {
	b, lights := kitchenBridge(t)
	// Every recall 404s, as it would if the scene had been deleted.
	b.FailNextRecallsWith(100, http.StatusNotFound)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	// Long enough for the first two backoffs to have fired, had there been any.
	settle()
	settle()
	settle()

	if got := b.Attempts(); got != 1 {
		t.Fatalf("a 404 must not be retried, got %d attempts", got)
	}
}

// TestRetriesAreBoundedAndReleaseTheRoom: retries must give up, and giving up
// must not leave the room suppressed. A room that is never released is worse
// than one that was never recalled - the user cannot fix it with a switch.
func TestRetriesAreBoundedAndReleaseTheRoom(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.FailNextRecallsWith(100, http.StatusServiceUnavailable)
	startKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	waitFor(t, 20*time.Second, "the retries to be exhausted", func() bool {
		return b.Attempts() >= len(recallBackoff)+1
	})
	settle()

	if got := b.Attempts(); got != len(recallBackoff)+1 {
		t.Fatalf("expected exactly %d attempts, got %d", len(recallBackoff)+1, got)
	}

	// The bridge recovers. A fresh trigger must work, which it cannot if the
	// last failure left lastRecall or suppressUntil armed.
	b.FailNextRecallsWith(0, 0)
	b.SwitchLight(lights[1], true)
	waitForRecalls(t, b, 1)
}

// TestWedgedRecallTimesOutRatherThanHangingForever: hue.Client deliberately has
// no http.Client.Timeout, because it would also cap the event stream. Without a
// per-request deadline in its place, a bridge that accepts a recall and then
// never answers would hold the sender forever and the room would never recover.
func TestWedgedRecallTimesOutRatherThanHangingForever(t *testing.T) {
	b, lights := kitchenBridge(t)
	// Far longer than the timeout, so every attempt is abandoned rather than
	// answered.
	b.DelayRecalls(30 * time.Second)

	cfg := testConfig()
	reg := registry.New()
	k := New(b.ClientWithTimeout(300*time.Millisecond), reg, cfg, testLogger(t), false)
	startPreparedKeeper(t, b, k, reg)

	b.SwitchLight(lights[0], true)

	// Each attempt is abandoned after the timeout and retried, so the bridge
	// sees the full set. Without the deadline the first would never return.
	waitFor(t, 20*time.Second, "the wedged recall to time out and retry", func() bool {
		return b.Attempts() >= len(recallBackoff)+1
	})
	if got := b.Recalls(); len(got) != 0 {
		t.Fatalf("no attempt was ever answered, so none should be recorded: %v", got)
	}
}

// TestASlowRecallDoesNotLoseAnotherRoom: recalls leave the dispatch goroutine,
// so a room stuck on a slow bridge must not take a second room down with it.
func TestASlowRecallDoesNotLoseAnotherRoom(t *testing.T) {
	b := fake.NewBridge(t)
	kitchen := b.AddRoom("room-kitchen", "Kitchen", "Ceiling")
	b.AddSmartScene(kitchenScene, "Natural Light", "room-kitchen", hue.TypeRoom)
	bedroom := b.AddRoom("room-bedroom", "Bedroom", "Lamp")
	b.AddSmartScene(bedroomScene, "Natural Light", "room-bedroom", hue.TypeRoom)
	b.DelayRecalls(time.Second)
	startKeeper(t, b, nil)

	b.SwitchLight(kitchen[0], true)
	waitFor(t, 5*time.Second, "the first recall to reach the bridge", func() bool {
		return b.Attempts() >= 1
	})

	// Raised while the kitchen is still on the wire.
	b.SwitchLight(bedroom[0], true)
	waitForRecalls(t, b, 2)

	got := b.Recalls()
	if len(got) != 2 || got[0] != kitchenScene || got[1] != bedroomScene {
		t.Fatalf("expected both rooms recalled in order, got %v", got)
	}
}

// TestPowerRestoreKeepsItsExemptionWhenFoldedIntoAPendingRecall: a
// power_restored trigger arriving for a group that is already pending is
// folded into the existing entry, which keeps the reason it was created with.
// The exemption from commit's "is any light still on" veto has to survive that
// fold, or the restore is silently vetoed and dropped with no retry - the lamp
// that came back on mains is never written into the registry, so the veto has
// nothing to see.
func TestPowerRestoreKeepsItsExemptionWhenFoldedIntoAPendingRecall(t *testing.T) {
	b, lights := kitchenBridge(t) // every light off
	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	pending := map[string]*pendingRecall{}
	light := lights[0]

	// A light flicks on, creating the entry, and goes straight back off.
	if !k.schedule(pending, trigger{kind: kindLight, id: light, reason: reasonSwitchedOn}) {
		t.Fatal("the first trigger should have created a pending entry")
	}
	// Then a lamp's mains comes back, folding into that same entry.
	k.schedule(pending, trigger{kind: kindLight, id: light, reason: reasonPowerRestored})

	p, ok := pending["room-kitchen"]
	if !ok {
		t.Fatal("the group should still be pending")
	}
	if !p.skipsOnCheck() {
		t.Fatal("a folded power_restored trigger must carry its exemption onto the entry")
	}
	if !k.commit(p) {
		t.Fatal("the restore was vetoed: no light is on in the registry, which is exactly what power_restored is exempt from")
	}
}

// --- suppression across groups -------------------------------------------------

// mixedZoneBridge builds the one topology where arming a recall and attributing
// its echo can disagree: a zone holding a room's lights alongside a light that
// is in no room, which is the only way GroupForLight ever picks a zone at all.
//
// The fake can only create a light by putting it in a room, so the strip starts
// in one. A caller that needs it genuinely roomless - the way an unassigned lamp
// looks on a real bridge - deletes that room afterwards.
func mixedZoneBridge(t *testing.T) (b *fake.Bridge, kitchen, strip []string) {
	t.Helper()
	b = fake.NewBridge(t)
	kitchen = b.AddRoom("room-kitchen", "Kitchen", "Ceiling", "Counter")
	b.AddSmartScene(kitchenScene, "Natural Light", "room-kitchen", hue.TypeRoom)
	strip = b.AddRoom("room-strip", "Unassigned", "Strip")
	b.AddZone("zone-downstairs", "Downstairs", append(append([]string{}, kitchen...), strip...)...)
	b.AddSmartScene(zoneScene, "Natural Light", "zone-downstairs", hue.TypeZone)
	return b, kitchen, strip
}

// TestAZoneRecallDoesNotFanOutIntoItsRooms: recalling a zone lights every light
// in it, and each of those on-events is attributed through GroupForLight, which
// prefers a light's room. Suppression armed on the zone alone covered none of
// them, so one zone recall became a recall of every room in that zone.
func TestAZoneRecallDoesNotFanOutIntoItsRooms(t *testing.T) {
	b, _, strip := mixedZoneBridge(t)
	b.EchoOnRecall = true
	_, reg := startKeeper(t, b, nil)

	// The strip loses its room, so the zone is what governs it.
	b.Publish("delete", map[string]any{"id": "room-strip", "type": hue.TypeRoom})
	waitFor(t, 5*time.Second, "the strip to become roomless", func() bool {
		g, ok := reg.GroupForLight(strip[0])
		return ok && g.ID == "zone-downstairs"
	})

	b.SwitchLight(strip[0], true)
	waitForRecalls(t, b, 1)

	// The kitchen's two lights have just reported themselves on, off the back
	// of our own recall. Neither may cause one.
	settle()
	settle()

	if got := b.Recalls(); len(got) != 1 || got[0] != zoneScene {
		t.Fatalf("a zone recall fanned out: expected one recall of %s, got %v", zoneScene, got)
	}
}

// TestAFailedZoneRecallReleasesEveryGroupItArmed is the other half of the same
// change: commit now arms more than one group, so rollback has to give back
// exactly that set. An asymmetry here is worse than the bug - one refused zone
// recall would leave every room in the zone silently suppressed, with no retry
// of its own to recover it.
func TestAFailedZoneRecallReleasesEveryGroupItArmed(t *testing.T) {
	b, kitchen, _ := mixedZoneBridge(t)
	// The zone needs a light on, or commit vetoes the recall before arming.
	b.SetLightStateSilently(kitchen[0], true)
	b.FailNextRecalls(1)

	reg := registry.New()
	k := New(b.Client(), reg, testConfig(), testLogger(t), false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := k.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}

	p := &pendingRecall{recall: recall{
		groupID: "zone-downstairs", groupName: "Downstairs",
		sceneID: zoneScene, sceneName: "Natural Light", reason: reasonSwitchedOn,
	}}
	if !k.commit(p) {
		t.Fatal("the zone recall should have been admitted")
	}
	<-k.sendQ
	if _, ok := k.suppressUntil["room-kitchen"]; !ok {
		t.Fatal("commit armed the zone but not the room its echo is attributed to")
	}

	err := k.client.RecallSmartScene(ctx, p.sceneID)
	if err == nil {
		t.Fatal("the bridge was supposed to refuse this recall")
	}
	k.finish(map[string]*pendingRecall{}, outcome{pending: p, err: err})

	if len(k.suppressUntil) != 0 {
		t.Fatalf("a refused recall left groups suppressed: %v", k.suppressUntil)
	}
	if len(k.lastRecall) != 0 {
		t.Fatalf("a refused recall left the recall floor armed: %v", k.lastRecall)
	}
}

// --- construction and crashes --------------------------------------------------

// TestNewToleratesANilConfig: New nil-checks the logger, and a nil config used
// to sail straight past it and panic on the dispatch goroutine at the first
// trigger - seconds later, on a stack that names nobody who built the Keeper.
func TestNewToleratesANilConfig(t *testing.T) {
	k := New(nil, nil, nil, testLogger(t), false)
	if got, want := k.coalesceWindow(), config.Default().CoalesceWindow.Duration(); got != want {
		t.Fatalf("a nil config should fall back to the defaults, got %s want %s", got, want)
	}
}

// TestAPanickingWorkerIsLoggedAndStillDies: the recover on Run's goroutines is
// there to name which one went, not to keep it going. Absorbing the panic would
// leave the daemon running with no dispatch goroutine - alive, quiet, and
// recalling nothing - which is worse than the crash the service manager can see.
func TestAPanickingWorkerIsLoggedAndStillDies(t *testing.T) {
	var buf bytes.Buffer
	k := &Keeper{log: slog.New(slog.NewTextHandler(&buf, nil))}

	rethrown := func() (v any) {
		defer func() { v = recover() }()
		defer k.logPanic("dispatch")
		panic("bridge sent something we did not expect")
	}()

	if rethrown == nil {
		t.Fatal("logPanic swallowed the panic instead of logging it and letting it through")
	}
	out := buf.String()
	for _, want := range []string{"goroutine=dispatch", "did not expect", "stack="} {
		if !strings.Contains(out, want) {
			t.Errorf("the panic log omitted %q:\n%s", want, out)
		}
	}
}

// startStoppableKeeper runs a keeper the way startKeeper does, but hands the
// stop back instead of only registering it as cleanup, so a test can assert on
// what shutdown itself did.
//
// stop is idempotent: it is both the test's own call and the cleanup.
func startStoppableKeeper(t *testing.T, b *fake.Bridge, cfg *config.Config) (*registry.Registry, func()) {
	t.Helper()
	if cfg == nil {
		cfg = testConfig()
	}
	reg := registry.New()
	k := New(b.Client(), reg, cfg, testLogger(t), false)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = k.Run(ctx)
	}()
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("keeper did not shut down")
		}
	}
	t.Cleanup(stop)

	if !b.WaitForSubscriber(5 * time.Second) {
		t.Fatal("keeper never connected to the event stream")
	}
	waitFor(t, 5*time.Second, "initial sync", func() bool {
		lights, rooms, _, scenes := reg.Counts()
		return lights > 0 && rooms > 0 && scenes > 0
	})
	return reg, stop
}

// TestAPendingRecallIsSentAtShutdown: a recall waiting out its coalescing
// window when the signal arrives has no second chance. The light that caused it
// is already on, so the next start sees it as it has always been, no off->on
// edge ever occurs, and the room keeps whatever state something else left it
// in. A restart in the seconds after somebody walks into a room is exactly that
// window.
func TestAPendingRecallIsSentAtShutdown(t *testing.T) {
	b, lights := kitchenBridge(t)
	cfg := testConfig()
	// Long enough that the entry is certainly still waiting when we stop.
	cfg.CoalesceWindow = config.Duration(2 * time.Second)
	cfg.CoalesceMax = config.Duration(4 * time.Second)
	reg, stop := startStoppableKeeper(t, b, cfg)

	b.SwitchLight(lights[0], true)
	waitFor(t, 5*time.Second, "the switch-on to be seen", func() bool {
		on, known := reg.LightIsOn(lights[0])
		return known && on
	})
	// The registry is written just before the trigger is queued; this is
	// dispatch's moment to fold it into pending.
	time.Sleep(100 * time.Millisecond)
	if n := len(b.Recalls()); n != 0 {
		t.Fatalf("the recall should still have been waiting out its window, got %d", n)
	}

	stop()
	if n := len(b.Recalls()); n != 1 {
		t.Fatalf("a recall pending at shutdown must still be sent, got %d", n)
	}
}

// TestARecallOnTheWireIsNotCutOffByShutdown: the request is the sender's, and
// cancelling it with everything else abandoned a recall the bridge had already
// accepted - the fake stops short of applying one whose caller has gone, just
// as a real bridge may or may not have started on it.
func TestARecallOnTheWireIsNotCutOffByShutdown(t *testing.T) {
	b, lights := kitchenBridge(t)
	b.DelayRecalls(400 * time.Millisecond)
	_, stop := startStoppableKeeper(t, b, nil)

	b.SwitchLight(lights[0], true)
	waitFor(t, 5*time.Second, "the recall to reach the bridge", func() bool {
		return b.Attempts() > 0
	})

	stop()
	if n := len(b.Recalls()); n != 1 {
		t.Fatalf("a recall already on the wire must be allowed to finish, got %d", n)
	}
}

// TestShutdownIsPromptWithNothingToDrain: the grace period is a ceiling for the
// case that needs it, not a tax on every stop. A daemon that took it every time
// would turn an ordinary `systemctl restart` into three seconds of nothing,
// which is how people learn to reach for kill -9.
func TestShutdownIsPromptWithNothingToDrain(t *testing.T) {
	b, _ := kitchenBridge(t)
	_, stop := startStoppableKeeper(t, b, nil)

	start := time.Now()
	stop()
	if took := time.Since(start); took > time.Second {
		t.Fatalf("shutdown with nothing pending took %s", took.Round(time.Millisecond))
	}
}

// TestRunReturnsWhenTheStreamGivesUp: Stream hands back the errors retrying
// cannot fix - a revoked application key here - precisely so the service
// manager sees a failure rather than a process that is alive and doing nothing.
// Run has to hand them on: waiting on goroutines that watch only the caller's
// context meant it never returned at all, and the caller never got to
// rediscover the bridge or report the failure.
func TestRunReturnsWhenTheStreamGivesUp(t *testing.T) {
	b, _ := kitchenBridge(t)
	client := hue.New(hue.Options{
		BaseURL:           b.URL(),
		AppKey:            "not-the-key-the-bridge-wants",
		Insecure:          true,
		RequestsPerSecond: 10000,
	})
	k := New(client, registry.New(), testConfig(), testLogger(t), false)

	done := make(chan error, 1)
	go func() { done <- k.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a revoked application key must not look like a clean stop")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned after the stream gave up")
	}
}
