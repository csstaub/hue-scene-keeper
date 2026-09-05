package registry

import (
	"encoding/json"
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
// real Hue lamps are modelled: a room's children are devices, not lights.
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
	group, ok := r.GroupForLight("light1")
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
	group, _ := r.GroupForLight("light1")
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
	group, ok := r.GroupForLight("orphan")
	if !ok || group.ID != "zoneA" {
		t.Fatalf("expected the zone as fallback, got %+v (ok=%v)", group, ok)
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
			if got := r.GroupShadowed(tt.group); got != tt.want {
				t.Errorf("GroupShadowed(%q) = %v, want %v", tt.group, got, tt.want)
			}
		})
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
	if r.GroupShadowed("mixed") {
		t.Error("a zone holding one roomless light is still selectable")
	}
}
