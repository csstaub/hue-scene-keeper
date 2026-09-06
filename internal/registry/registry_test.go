package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/csstaub/hue-scene-keeper/internal/hue"
)

func raw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func apply(r *Registry, action string, msgs ...json.RawMessage) {
	r.Apply([]hue.Event{{Type: action, Data: msgs}})
}

// seed builds a room containing one device that owns one light, which is how
// real Hue lamps are modeled: a room's children are devices, not lights.
func seed(t *testing.T) *Registry {
	t.Helper()
	r := New()
	apply(r, "add",
		raw(t, hue.Device{
			ID: "dev1", Type: hue.TypeDevice,
			Metadata: &hue.Metadata{Name: "Ceiling"},
			Services: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
		}),
		raw(t, hue.Light{
			ID: "light1", Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: "dev1", RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: "Ceiling"},
			On:       &hue.On{On: false},
		}),
		raw(t, hue.Group{
			ID: "room1", Type: hue.TypeRoom,
			Metadata: &hue.Metadata{Name: "Kitchen"},
			Children: []hue.ResourceIdentifier{{RID: "dev1", RType: hue.TypeDevice}},
		}),
		raw(t, hue.SmartScene{
			ID: "scene1", Type: hue.TypeSmartScene,
			Metadata: &hue.Metadata{Name: "Natural Light"},
			Group:    hue.ResourceIdentifier{RID: "room1", RType: hue.TypeRoom},
			State:    hue.SmartSceneInactive,
		}),
	)
	return r
}

// TestRoomResolvesThroughDevice is the mapping that is easy to get wrong:
// light -> owner device -> room, never light -> room directly.
func TestRoomResolvesThroughDevice(t *testing.T) {
	r := seed(t)
	group, ok := r.GroupForLight("light1", nil)
	if !ok {
		t.Fatal("light1 resolved to no group")
	}
	if group.ID != "room1" || group.Type != hue.TypeRoom {
		t.Fatalf("expected room1, got %+v", group)
	}
}

// TestZoneChildrenAreLightsDirectly is the other half of that asymmetry.
func TestZoneChildrenAreLightsDirectly(t *testing.T) {
	r := seed(t)
	apply(r, "add", raw(t, hue.Group{
		ID: "zone1", Type: hue.TypeZone,
		Metadata: &hue.Metadata{Name: "Desk"},
		Children: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
	}))

	if got := r.LightIDsInGroup("zone1"); len(got) != 1 || got[0] != "light1" {
		t.Fatalf("zone lights: %v", got)
	}
	if got := r.ZoneIDsForLight("light1"); len(got) != 1 || got[0] != "zone1" {
		t.Fatalf("zones for light: %v", got)
	}
	// The room still wins for scene selection.
	group, _ := r.GroupForLight("light1", nil)
	if group.ID != "room1" {
		t.Fatalf("room should win over zone, got %s", group.ID)
	}
}

func TestZoneUsedWhenLightHasNoRoom(t *testing.T) {
	r := New()
	apply(r, "add",
		raw(t, hue.Light{
			ID: "orphan", Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: "devX", RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: "Orphan"},
		}),
		raw(t, hue.Group{
			ID: "zoneA", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "Hallway"},
			Children: []hue.ResourceIdentifier{{RID: "orphan", RType: hue.TypeLight}},
		}),
	)
	group, ok := r.GroupForLight("orphan", nil)
	if !ok || group.ID != "zoneA" {
		t.Fatalf("expected the zone as fallback, got %+v (ok=%v)", group, ok)
	}
}

// TestExcludedRoomCedesLightToZone: exclusion is an input to resolution, not a
// veto after it. An excluded room hands its lights to the first non-excluded
// zone holding them, which is what lets a zone carve a light out of a room the
// daemon otherwise leaves alone.
func TestExcludedRoomCedesLightToZone(t *testing.T) {
	r := seed(t)
	apply(r, "add", raw(t, hue.Group{
		ID: "zone1", Type: hue.TypeZone,
		Metadata: &hue.Metadata{Name: "Desk"},
		Children: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
	}))

	excludeRoom := func(id string) bool { return id == "room1" }
	group, ok := r.GroupForLight("light1", excludeRoom)
	if !ok || group.ID != "zone1" {
		t.Fatalf("expected the zone once the room is excluded, got %+v (ok=%v)", group, ok)
	}

	// With the zone excluded too, the light belongs to nothing.
	excludeBoth := func(id string) bool { return id == "room1" || id == "zone1" }
	if group, ok := r.GroupForLight("light1", excludeBoth); ok {
		t.Fatalf("everything excluded, yet resolved to %s", group.ID)
	}
}

// TestExcludedZoneIsSkippedInFallback: the zone loop honors exclusions, and
// the pick among several zones stays deterministic (sorted by id).
func TestExcludedZoneIsSkippedInFallback(t *testing.T) {
	r := New()
	apply(r, "add",
		raw(t, hue.Light{
			ID: "orphan", Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: "devX", RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: "Orphan"},
		}),
		raw(t, hue.Group{
			ID: "zoneA", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "First"},
			Children: []hue.ResourceIdentifier{{RID: "orphan", RType: hue.TypeLight}},
		}),
		raw(t, hue.Group{
			ID: "zoneB", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "Second"},
			Children: []hue.ResourceIdentifier{{RID: "orphan", RType: hue.TypeLight}},
		}),
	)

	if group, ok := r.GroupForLight("orphan", nil); !ok || group.ID != "zoneA" {
		t.Fatalf("expected zoneA (first by id), got %+v (ok=%v)", group, ok)
	}
	excludeA := func(id string) bool { return id == "zoneA" }
	if group, ok := r.GroupForLight("orphan", excludeA); !ok || group.ID != "zoneB" {
		t.Fatalf("expected zoneB with zoneA excluded, got %+v (ok=%v)", group, ok)
	}
}

// TestPartialUpdateDoesNotClobber: update events carry only changed fields, so
// merging must not erase the name or owner we already know.
func TestPartialUpdateDoesNotClobber(t *testing.T) {
	r := seed(t)
	apply(r, "update", json.RawMessage(`{"id":"light1","type":"light","on":{"on":true}}`))

	light, ok := r.Light("light1")
	if !ok {
		t.Fatal("light disappeared")
	}
	if light.Name() != "Ceiling" {
		t.Errorf("name clobbered by partial update: %q", light.Name())
	}
	if light.Owner.RID != "dev1" {
		t.Errorf("owner clobbered by partial update: %q", light.Owner.RID)
	}
	if on, known := r.LightIsOn("light1"); !known || !on {
		t.Errorf("on-state not applied: on=%v known=%v", on, known)
	}
}

func TestSmartSceneLookupIsCaseInsensitive(t *testing.T) {
	r := seed(t)
	for _, name := range []string{"Natural Light", "natural light", "  NATURAL LIGHT  "} {
		if _, ok := r.SmartSceneForGroup("room1", name); !ok {
			t.Errorf("lookup failed for %q", name)
		}
	}
	if _, ok := r.SmartSceneForGroup("room1", "Concentrate"); ok {
		t.Error("matched a scene that does not exist")
	}
}

func TestDeleteRemovesResource(t *testing.T) {
	r := seed(t)
	apply(r, "delete", json.RawMessage(`{"id":"light1","type":"light"}`))
	if _, ok := r.Light("light1"); ok {
		t.Fatal("light was not deleted")
	}
}

func TestLightIDsForDevice(t *testing.T) {
	r := seed(t)
	if got := r.LightIDsForDevice("dev1"); len(got) != 1 || got[0] != "light1" {
		t.Fatalf("device lights: %v", got)
	}
}

func TestConnectivityMerge(t *testing.T) {
	r := New()
	apply(r, "add", raw(t, hue.ZigbeeConnectivity{
		ID: "c1", Type: hue.TypeZigbeeConnectivity,
		Owner:  hue.ResourceIdentifier{RID: "dev1", RType: hue.TypeDevice},
		Status: hue.StatusConnected,
	}))
	apply(r, "update", json.RawMessage(`{"id":"c1","type":"zigbee_connectivity","status":"connectivity_issue"}`))

	conn, ok := r.Connectivity("c1")
	if !ok {
		t.Fatal("connectivity missing")
	}
	if conn.Status != hue.StatusConnectivityIssue {
		t.Errorf("status = %q", conn.Status)
	}
	if conn.Owner.RID != "dev1" {
		t.Errorf("owner clobbered: %q", conn.Owner.RID)
	}
}

// TestGroupShadowed: a zone whose lights all sit in a room can never be the
// group GroupForLight returns, so anything the config says about it is inert.
// This is what lets the config layer warn instead of failing silently.
func TestGroupShadowed(t *testing.T) {
	r := seed(t)
	// A zone over the same light the room already owns.
	apply(r, "add", raw(t, hue.Group{
		ID: "zone1", Type: hue.TypeZone,
		Metadata: &hue.Metadata{Name: "Desk"},
		Children: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
	}))
	// An empty zone, and a zone over a light that belongs to no room.
	apply(r, "add",
		raw(t, hue.Group{
			ID: "zone2", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "Empty"},
		}),
		raw(t, hue.Light{
			ID: "orphan", Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: "devX", RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: "Orphan"},
		}),
		raw(t, hue.Group{
			ID: "zone3", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "Hallway"},
			Children: []hue.ResourceIdentifier{{RID: "orphan", RType: hue.TypeLight}},
		}),
	)

	tests := []struct {
		name  string
		group string
		want  bool
	}{
		{"a room owns its lights outright", "room1", false},
		{"a zone over a light that has a room", "zone1", true},
		{"an empty zone has nothing to shadow it", "zone2", false},
		{"a zone over a roomless light is selectable", "zone3", false},
		{"an unknown group", "nope", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.GroupShadowed(tt.group, nil); got != tt.want {
				t.Errorf("GroupShadowed(%q) = %v, want %v", tt.group, got, tt.want)
			}
		})
	}
}

// TestGroupShadowedUnderExclusions: an excluded room cedes its lights, which
// un-shadows a zone carving them out - and judged with its own exclusion
// peeled off (the config layer's except-self rule), excluding that zone is
// then meaningful rather than inert.
func TestGroupShadowedUnderExclusions(t *testing.T) {
	r := seed(t)
	apply(r, "add", raw(t, hue.Group{
		ID: "zone1", Type: hue.TypeZone,
		Metadata: &hue.Metadata{Name: "Desk"},
		Children: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
	}))

	if !r.GroupShadowed("zone1", nil) {
		t.Fatal("with the room live, the zone should be shadowed")
	}
	excludeRoom := func(id string) bool { return id == "room1" }
	if r.GroupShadowed("zone1", excludeRoom) {
		t.Fatal("with the room excluded, the zone should be reachable")
	}
}

// TestGroupShadowedNeedsEveryLightCovered: one light the zone alone can claim
// is enough to make the zone reachable, so the warning must not fire.
func TestGroupShadowedNeedsEveryLightCovered(t *testing.T) {
	r := seed(t)
	apply(r, "add",
		raw(t, hue.Light{
			ID: "orphan", Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: "devX", RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: "Orphan"},
		}),
		raw(t, hue.Group{
			ID: "mixed", Type: hue.TypeZone,
			Metadata: &hue.Metadata{Name: "Mixed"},
			Children: []hue.ResourceIdentifier{
				{RID: "light1", RType: hue.TypeLight},
				{RID: "orphan", RType: hue.TypeLight},
			},
		}),
	)
	if r.GroupShadowed("mixed", nil) {
		t.Error("a zone holding one roomless light is still selectable")
	}
}

// TestDeletedLightResolvesToNoGroup: a zone still lists a deleted light among
// its children until the zone itself updates, so the zone fallback would keep
// resolving a light the cache no longer holds - and that makes GroupShadowed
// read the zone as reachable, swallowing the "exclusion has no effect" warning
// the config layer prints.
func TestDeletedLightResolvesToNoGroup(t *testing.T) {
	r := seed(t)
	apply(r, "add", raw(t, hue.Group{
		ID: "zone1", Type: hue.TypeZone,
		Metadata: &hue.Metadata{Name: "Desk"},
		Children: []hue.ResourceIdentifier{{RID: "light1", RType: hue.TypeLight}},
	}))
	apply(r, "delete", json.RawMessage(`{"id":"light1","type":"light"}`))

	if group, ok := r.GroupForLight("light1", nil); ok {
		t.Errorf("a deleted light still resolves to %s", group.ID)
	}
	if !r.GroupShadowed("zone1", nil) {
		t.Error("a zone whose only light is gone is not reachable; the warning must still fire")
	}
}

// syncClient serves canned CLIP v2 envelopes for the resource types Sync
// fetches. A type with no entries listed answers with an empty data array,
// which is what a bridge holding none of that resource returns.
func syncClient(t *testing.T, byType map[string][]string) *hue.Client {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		entries := byType[path.Base(r.URL.Path)]
		body := fmt.Sprintf(`{"errors":[],"data":[%s]}`, strings.Join(entries, ","))
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("writing %s: %v", r.URL.Path, err)
		}
	}))
	t.Cleanup(srv.Close)
	return hue.New(hue.Options{
		BaseURL: srv.URL, AppKey: "k", Insecure: true, RequestsPerSecond: 10000,
	})
}

// TestSyncSkipsAnUnusableResource: one resource whose shape we cannot read used
// to fail the whole sync, which fails the resync, which drops the stream - and
// the same resource comes back on every reconnect, so the daemon parks at the
// backoff cap doing nothing at all.
func TestSyncSkipsAnUnusableResource(t *testing.T) {
	c := syncClient(t, map[string][]string{
		hue.TypeLight: {
			// owner as a string rather than a resource identifier, which is
			// the shape change this guards against.
			`{"id":"bad","type":"light","owner":"dev1"}`,
			`{"id":"light1","type":"light","owner":{"rid":"dev1","rtype":"device"},"on":{"on":true}}`,
		},
		hue.TypeRoom: {
			`{"id":"room1","type":"room","children":[{"rid":"dev1","rtype":"device"}]}`,
		},
	})

	r := New()
	if err := r.Sync(context.Background(), c); err != nil {
		t.Fatalf("sync failed on one unreadable resource: %v", err)
	}
	if _, ok := r.Light("light1"); !ok {
		t.Error("the readable light was dropped along with the bad one")
	}
	if _, ok := r.Light("bad"); ok {
		t.Error("the unreadable light was cached")
	}
	// A type the bridge holds nothing of is not an error.
	if _, rooms, zones, _ := r.Counts(); rooms != 1 || zones != 0 {
		t.Errorf("rooms=%d zones=%d", rooms, zones)
	}
}

// TestSyncFailsWhenAWholeTypeIsUnusable: skipping outliers must not extend to
// pretending a resource type the bridge does hold is empty.
func TestSyncFailsWhenAWholeTypeIsUnusable(t *testing.T) {
	c := syncClient(t, map[string][]string{
		hue.TypeLight: {
			`{"id":"l1","type":"light","owner":"dev1"}`,
			`{"id":"l2","type":"light","owner":"dev2"}`,
		},
	})

	r := New()
	if err := r.Sync(context.Background(), c); err == nil {
		t.Fatal("sync succeeded with no usable light at all")
	}
}

// TestSyncRejectsResourcesWithoutAnID: the stream path already drops these, and
// caching one files a zero-value resource under "" where Group("") reports it
// as found.
func TestSyncRejectsResourcesWithoutAnID(t *testing.T) {
	c := syncClient(t, map[string][]string{
		hue.TypeLight: {
			`{"type":"light","owner":{"rid":"dev1","rtype":"device"}}`,
			`{"id":"light1","type":"light","owner":{"rid":"dev1","rtype":"device"}}`,
		},
		hue.TypeRoom: {
			`{"type":"room","metadata":{"name":"Nameless"}}`,
			`{"id":"room1","type":"room","metadata":{"name":"Kitchen"}}`,
		},
		hue.TypeSmartScene: {
			`{"type":"smart_scene","group":{"rid":"room1","rtype":"room"}}`,
			`{"id":"scene1","type":"smart_scene","group":{"rid":"room1","rtype":"room"}}`,
		},
	})

	r := New()
	if err := r.Sync(context.Background(), c); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if _, ok := r.Light(""); ok {
		t.Error(`Light("") found an idless light`)
	}
	if _, ok := r.Group(""); ok {
		t.Error(`Group("") found an idless room`)
	}
	if _, ok := r.SmartScene(""); ok {
		t.Error(`SmartScene("") found an idless smart scene`)
	}
	if _, ok := r.Light("light1"); !ok {
		t.Error("the light with an id was dropped")
	}
}
