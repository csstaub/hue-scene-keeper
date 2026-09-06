// Package registry keeps an in-memory mirror of the bridge's resources and the
// lookups the keeper needs: which room a light lives in, and which smart scene
// belongs to that room.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/csstaub/hue-scene-keeper/internal/hue"
)

// errNoID marks a resource the bridge returned without an id. Caching it would
// file a zero-value resource under the "" key, where Group("") and SmartScene("")
// then report it as found.
var errNoID = errors.New("resource has no id")

// Registry is safe for concurrent use.
type Registry struct {
	mu sync.RWMutex

	lights  map[string]hue.Light
	devices map[string]hue.Device
	rooms   map[string]hue.Group
	zones   map[string]hue.Group
	scenes  map[string]hue.SmartScene
	conn    map[string]hue.ZigbeeConnectivity

	// Derived indexes, rebuilt whenever membership changes.
	deviceToRoom map[string]string
	lightToZones map[string][]string
}

// New returns an empty registry.
func New() *Registry {
	r := &Registry{}
	r.reset()
	return r
}

func (r *Registry) reset() {
	r.lights = map[string]hue.Light{}
	r.devices = map[string]hue.Device{}
	r.rooms = map[string]hue.Group{}
	r.zones = map[string]hue.Group{}
	r.scenes = map[string]hue.SmartScene{}
	r.conn = map[string]hue.ZigbeeConnectivity{}
	r.deviceToRoom = map[string]string{}
	r.lightToZones = map[string][]string{}
}

// Sync replaces the whole cache from the bridge. A full refetch is a handful of
// requests, which is cheaper than trying to repair the cache incrementally
// after a disconnect of unknown length.
func (r *Registry) Sync(ctx context.Context, c *hue.Client) error {
	type fetch struct {
		rtype string
		into  func(json.RawMessage) error
	}

	next := &Registry{}
	next.reset()

	fetches := []fetch{
		{hue.TypeDevice, func(raw json.RawMessage) error {
			var d hue.Device
			if err := json.Unmarshal(raw, &d); err != nil {
				return err
			}
			if d.ID == "" {
				return errNoID
			}
			next.devices[d.ID] = d
			return nil
		}},
		{hue.TypeLight, func(raw json.RawMessage) error {
			var l hue.Light
			if err := json.Unmarshal(raw, &l); err != nil {
				return err
			}
			if l.ID == "" {
				return errNoID
			}
			next.lights[l.ID] = l
			return nil
		}},
		{hue.TypeRoom, func(raw json.RawMessage) error {
			var g hue.Group
			if err := json.Unmarshal(raw, &g); err != nil {
				return err
			}
			if g.ID == "" {
				return errNoID
			}
			next.rooms[g.ID] = g
			return nil
		}},
		{hue.TypeZone, func(raw json.RawMessage) error {
			var g hue.Group
			if err := json.Unmarshal(raw, &g); err != nil {
				return err
			}
			if g.ID == "" {
				return errNoID
			}
			next.zones[g.ID] = g
			return nil
		}},
		{hue.TypeSmartScene, func(raw json.RawMessage) error {
			var s hue.SmartScene
			if err := json.Unmarshal(raw, &s); err != nil {
				return err
			}
			if s.ID == "" {
				return errNoID
			}
			next.scenes[s.ID] = s
			return nil
		}},
		{hue.TypeZigbeeConnectivity, func(raw json.RawMessage) error {
			var z hue.ZigbeeConnectivity
			if err := json.Unmarshal(raw, &z); err != nil {
				return err
			}
			if z.ID == "" {
				return errNoID
			}
			next.conn[z.ID] = z
			return nil
		}},
	}

	for _, f := range fetches {
		entries, err := c.GetResource(ctx, f.rtype)
		if err != nil {
			return fmt.Errorf("sync %s: %w", f.rtype, err)
		}
		skipped := 0
		for _, raw := range entries {
			if err := f.into(raw); err != nil {
				// One resource we cannot read must not cost us the sync.
				// Failing here fails Resync, which drops the stream, and the
				// same resource comes back on the reconnect - so the daemon
				// settles at the backoff cap, connecting and dropping forever
				// while doing nothing at all.
				skipped++
				slog.Warn("skipping unusable resource", "type", f.rtype, "err", err)
			}
		}
		// Nothing usable in a type the bridge did return entries for is a
		// shape change rather than one bad resource, and carrying on would
		// leave the cache quietly empty of a whole class of resource.
		if skipped > 0 && skipped == len(entries) {
			return fmt.Errorf("sync %s: none of the %d entries were usable", f.rtype, len(entries))
		}
	}
	next.reindex()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.lights, r.devices = next.lights, next.devices
	r.rooms, r.zones = next.rooms, next.zones
	r.scenes, r.conn = next.scenes, next.conn
	r.deviceToRoom, r.lightToZones = next.deviceToRoom, next.lightToZones
	return nil
}

// reindex rebuilds the derived lookups. Callers must hold the write lock (or
// own the registry exclusively, as Sync does).
func (r *Registry) reindex() {
	r.deviceToRoom = map[string]string{}
	for id, room := range r.rooms {
		// A room's children are devices, not lights.
		for _, child := range room.Children {
			if child.RType == hue.TypeDevice {
				r.deviceToRoom[child.RID] = id
			}
		}
	}
	r.lightToZones = map[string][]string{}
	for id, zone := range r.zones {
		// A zone's children are lights directly.
		for _, child := range zone.Children {
			if child.RType == hue.TypeLight {
				r.lightToZones[child.RID] = append(r.lightToZones[child.RID], id)
			}
		}
	}
}

// Apply merges a batch of stream events into the cache. It reports whether any
// membership changed, which the caller may use to log a topology change.
func (r *Registry) Apply(events []hue.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	structural := false
	for _, ev := range events {
		for _, raw := range ev.Data {
			if r.applyOne(ev.Type, raw) {
				structural = true
			}
		}
	}
	if structural {
		r.reindex()
	}
}

// applyOne merges a single resource, returning true when group membership may
// have changed.
func (r *Registry) applyOne(action string, raw json.RawMessage) bool {
	rtype := hue.PeekType(raw)
	del := action == "delete"

	switch rtype {
	case hue.TypeLight:
		var in hue.Light
		if json.Unmarshal(raw, &in) != nil || in.ID == "" {
			return false
		}
		if del {
			delete(r.lights, in.ID)
			return true
		}
		r.lights[in.ID] = mergeLight(r.lights[in.ID], in)
		return action == "add"

	case hue.TypeDevice:
		var in hue.Device
		if json.Unmarshal(raw, &in) != nil || in.ID == "" {
			return false
		}
		if del {
			delete(r.devices, in.ID)
			return true
		}
		cur := r.devices[in.ID]
		cur.ID, cur.Type = in.ID, hue.TypeDevice
		if in.Metadata != nil {
			cur.Metadata = in.Metadata
		}
		if in.Services != nil {
			cur.Services = in.Services
		}
		r.devices[in.ID] = cur
		return true

	case hue.TypeRoom, hue.TypeZone:
		var in hue.Group
		if json.Unmarshal(raw, &in) != nil || in.ID == "" {
			return false
		}
		target := r.rooms
		if rtype == hue.TypeZone {
			target = r.zones
		}
		if del {
			delete(target, in.ID)
			return true
		}
		cur := target[in.ID]
		cur.ID, cur.Type = in.ID, rtype
		if in.Metadata != nil {
			cur.Metadata = in.Metadata
		}
		if in.Children != nil {
			cur.Children = in.Children
		}
		if in.Services != nil {
			cur.Services = in.Services
		}
		target[in.ID] = cur
		return true

	case hue.TypeSmartScene:
		var in hue.SmartScene
		if json.Unmarshal(raw, &in) != nil || in.ID == "" {
			return false
		}
		if del {
			delete(r.scenes, in.ID)
			return true
		}
		cur := r.scenes[in.ID]
		cur.ID, cur.Type = in.ID, hue.TypeSmartScene
		if in.Metadata != nil {
			cur.Metadata = in.Metadata
		}
		if in.Group.RID != "" {
			cur.Group = in.Group
		}
		if in.State != "" {
			cur.State = in.State
		}
		r.scenes[in.ID] = cur
		return action == "add"

	case hue.TypeZigbeeConnectivity:
		var in hue.ZigbeeConnectivity
		if json.Unmarshal(raw, &in) != nil || in.ID == "" {
			return false
		}
		if del {
			delete(r.conn, in.ID)
			return false
		}
		cur := r.conn[in.ID]
		cur.ID, cur.Type = in.ID, hue.TypeZigbeeConnectivity
		if in.Owner.RID != "" {
			cur.Owner = in.Owner
		}
		if in.Status != "" {
			cur.Status = in.Status
		}
		r.conn[in.ID] = cur
		return false
	}
	return false
}

// mergeLight overlays a (possibly partial) update onto the cached light.
// Update events carry only the fields that changed, so absent fields must not
// clobber what we already know.
func mergeLight(cur, in hue.Light) hue.Light {
	cur.ID, cur.Type = in.ID, hue.TypeLight
	if in.Owner.RID != "" {
		cur.Owner = in.Owner
	}
	if in.Metadata != nil {
		cur.Metadata = in.Metadata
	}
	if in.On != nil {
		cur.On = in.On
	}
	return cur
}

// Light returns a cached light.
func (r *Registry) Light(id string) (hue.Light, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, ok := r.lights[id]
	return l, ok
}

// Lights returns every cached light, ordered by name.
func (r *Registry) Lights() []hue.Light {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]hue.Light, 0, len(r.lights))
	for _, l := range r.lights {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// LightIsOn reports the cached on-state of a light.
func (r *Registry) LightIsOn(id string) (on bool, known bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, ok := r.lights[id]
	if !ok || l.On == nil {
		return false, false
	}
	return l.On.On, true
}

// Rooms returns every room, ordered by name.
func (r *Registry) Rooms() []hue.Group { return r.groups(false) }

// Zones returns every zone, ordered by name.
func (r *Registry) Zones() []hue.Group { return r.groups(true) }

func (r *Registry) groups(zones bool) []hue.Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.rooms
	if zones {
		src = r.zones
	}
	out := make([]hue.Group, 0, len(src))
	for _, g := range src {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Group returns a room or zone by id.
func (r *Registry) Group(id string) (hue.Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if g, ok := r.rooms[id]; ok {
		return g, true
	}
	g, ok := r.zones[id]
	return g, ok
}

// GroupForLight resolves the group whose smart scene should govern a light.
//
// Rooms win over zones: a light belongs to exactly one room, but may belong to
// several zones, and the room is the grouping the Hue app builds Natural Light
// against.
//
// Exclusion feeds the resolution rather than vetoing its result: an excluded
// room cedes its lights to the first non-excluded zone holding them, which is
// what lets a zone carve a light out of a room the daemon otherwise leaves
// alone. A light whose room and zones are all excluded belongs to nothing.
// excluded may be nil, meaning nothing is excluded.
func (r *Registry) GroupForLight(lightID string, excluded func(string) bool) (hue.Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.groupForLightLocked(lightID, excluded)
}

func (r *Registry) groupForLightLocked(lightID string, excluded func(string) bool) (hue.Group, bool) {
	if excluded == nil {
		excluded = func(string) bool { return false }
	}
	// A light the cache no longer holds belongs to nothing. The zone fallback
	// below would otherwise still find it: a deleted light stays in its zone's
	// Children until the zone itself updates, so lightToZones keeps the
	// mapping, and the zone then reads as reachable - which is enough to
	// silence the GroupShadowed warning the config layer prints for an
	// exclusion that does nothing.
	l, ok := r.lights[lightID]
	if !ok {
		return hue.Group{}, false
	}
	if l.Owner.RID != "" {
		if roomID, ok := r.deviceToRoom[l.Owner.RID]; ok {
			if room, ok := r.rooms[roomID]; ok && !excluded(room.ID) {
				return room, true
			}
		}
	}
	// Fall back to a zone when the light is in no room at all, or when its
	// room is excluded and so has ceded the light.
	zoneIDs := append([]string(nil), r.lightToZones[lightID]...)
	sort.Strings(zoneIDs)
	for _, id := range zoneIDs {
		if z, ok := r.zones[id]; ok && !excluded(z.ID) {
			return z, true
		}
	}
	return hue.Group{}, false
}

// GroupShadowed reports whether every light in a group resolves to some *other*
// group, which makes the group unreachable: nothing the keeper does is ever
// attributed to it, so excluding it or pinning a scene to it is a no-op.
//
// On a real bridge this is the normal state of a zone whose rooms are not
// excluded: rooms win in GroupForLight and every light is in a room, so such a
// zone is selected only for the lights that somehow have no room at all. An
// excluded room cedes its lights, though, which is what un-shadows a zone that
// carves them out. The config layer uses this, with the exclusions it is
// resolving, to warn about an exclusion that would otherwise fail in complete
// silence.
//
// A group with no lights is not shadowed. Nothing is stealing its lights; it
// simply has none yet, and warning about an empty room the user is still
// furnishing would be noise.
func (r *Registry) GroupShadowed(groupID string, excluded func(string) bool) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lightIDs := r.lightIDsInGroupLocked(groupID)
	if len(lightIDs) == 0 {
		return false
	}
	for _, id := range lightIDs {
		if g, ok := r.groupForLightLocked(id, excluded); ok && g.ID == groupID {
			return false
		}
	}
	return true
}

// ZoneIDsForLight lists the zones a light belongs to.
func (r *Registry) ZoneIDsForLight(lightID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.lightToZones[lightID]...)
}

// LightIDsInGroup lists the lights belonging to a room or zone.
func (r *Registry) LightIDsInGroup(groupID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lightIDsInGroupLocked(groupID)
}

func (r *Registry) lightIDsInGroupLocked(groupID string) []string {
	var out []string
	if room, ok := r.rooms[groupID]; ok {
		for _, child := range room.Children {
			if child.RType != hue.TypeDevice {
				continue
			}
			for _, svc := range r.devices[child.RID].Services {
				if svc.RType == hue.TypeLight {
					out = append(out, svc.RID)
				}
			}
		}
	} else if zone, ok := r.zones[groupID]; ok {
		for _, child := range zone.Children {
			if child.RType == hue.TypeLight {
				out = append(out, child.RID)
			}
		}
	}
	sort.Strings(out)
	return out
}

// LightIDsForDevice lists the light services a device exposes.
func (r *Registry) LightIDsForDevice(deviceID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for _, svc := range r.devices[deviceID].Services {
		if svc.RType == hue.TypeLight {
			out = append(out, svc.RID)
		}
	}
	sort.Strings(out)
	return out
}

// Connectivity returns a cached zigbee connectivity service.
func (r *Registry) Connectivity(id string) (hue.ZigbeeConnectivity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	z, ok := r.conn[id]
	return z, ok
}

// SmartSceneForGroup finds the smart scene bound to a group whose name matches
// wantName, case-insensitively. The name is configurable because the Hue app
// localises it.
func (r *Registry) SmartSceneForGroup(groupID, wantName string) (hue.SmartScene, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	want := strings.ToLower(strings.TrimSpace(wantName))
	var matches []hue.SmartScene
	for _, s := range r.scenes {
		if s.Group.RID != groupID {
			continue
		}
		if strings.ToLower(strings.TrimSpace(s.Name())) == want {
			matches = append(matches, s)
		}
	}
	if len(matches) == 0 {
		return hue.SmartScene{}, false
	}
	// Deterministic pick if a bridge somehow holds duplicates.
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	return matches[0], true
}

// SmartScene returns a smart scene by id.
func (r *Registry) SmartScene(id string) (hue.SmartScene, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.scenes[id]
	return s, ok
}

// SmartScenesForGroup lists every smart scene bound to a group, by name.
func (r *Registry) SmartScenesForGroup(groupID string) []hue.SmartScene {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []hue.SmartScene
	for _, s := range r.scenes {
		if s.Group.RID == groupID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Counts reports cache sizes, for startup logging.
func (r *Registry) Counts() (lights, rooms, zones, smartScenes int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.lights), len(r.rooms), len(r.zones), len(r.scenes)
}

// OnLightIDs returns the set of lights currently believed to be on.
//
// The keeper snapshots this across a reconnect: a light that came on while the
// stream was down produces no off->on edge, because the resync simply records
// it as already on. Diffing two snapshots recovers exactly those lights.
func (r *Registry) OnLightIDs() map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]bool, len(r.lights))
	for id, l := range r.lights {
		if l.On != nil && l.On.On {
			out[id] = true
		}
	}
	return out
}

// DisconnectedDeviceIDs returns the devices whose radio the bridge currently
// reports as anything other than connected.
//
// The keeper snapshots this across a reconnect for the same reason it snapshots
// OnLightIDs: a lamp whose mains came back while the stream was down sends no
// `connected` event anyone was listening for, and the resync then records it as
// connected as though it always had been. Devices with no status yet are left
// out - there is no transition to recover from.
func (r *Registry) DisconnectedDeviceIDs() map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]bool{}
	for _, c := range r.conn {
		if c.Status == "" || c.Status == hue.StatusConnected {
			continue
		}
		if device := c.Owner.RID; device != "" {
			out[device] = true
		}
	}
	return out
}

// GroupHasLightOn reports whether any light in a room or zone is on.
func (r *Registry) GroupHasLightOn(groupID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range r.lightIDsInGroupLocked(groupID) {
		if l, ok := r.lights[id]; ok && l.On != nil && l.On.On {
			return true
		}
	}
	return false
}
