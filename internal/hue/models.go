// Package hue is a minimal client for the Philips Hue CLIP API v2.
//
// It covers only what hue-scene-keeper needs. Bridge discovery, link-button
// pairing, reading a handful of resource types, recalling smart scenes, and
// consuming the bridge's server-sent event stream.
package hue

import "encoding/json"

// Resource types we care about.
const (
	TypeDevice             = "device"
	TypeLight              = "light"
	TypeRoom               = "room"
	TypeZone               = "zone"
	TypeSmartScene         = "smart_scene"
	TypeZigbeeConnectivity = "zigbee_connectivity"
	TypeGroupedLight       = "grouped_light"
)

// Zigbee connectivity statuses. A transition into StatusConnected from any of
// the others is how we learn that a lamp's mains power came back.
const (
	StatusConnected              = "connected"
	StatusDisconnected           = "disconnected"
	StatusConnectivityIssue      = "connectivity_issue"
	StatusUnidirectionalIncoming = "unidirectional_incoming"
	StatusPendingDiscovery       = "pending_discovery"
)

// Smart scene states.
const (
	SmartSceneActive   = "active"
	SmartSceneInactive = "inactive"
)

// ResourceIdentifier is the bridge's cross-reference between resources.
type ResourceIdentifier struct {
	RID   string `json:"rid"`
	RType string `json:"rtype"`
}

// Metadata carries the user-facing name of a resource.
type Metadata struct {
	Name      string `json:"name"`
	Archetype string `json:"archetype,omitempty"`
}

// On is the light on/off feature.
type On struct {
	On bool `json:"on"`
}

// Light is a light service. Pointer fields distinguish "absent from this
// partial update event" from "present and false", which matters because the
// bridge sends only changed fields in update events.
type Light struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Owner    ResourceIdentifier `json:"owner"`
	Metadata *Metadata          `json:"metadata,omitempty"`
	On       *On                `json:"on,omitempty"`
}

// Name returns the light's name, or its id when the resource was a partial
// update that carried no metadata.
func (l Light) Name() string {
	if l.Metadata != nil && l.Metadata.Name != "" {
		return l.Metadata.Name
	}
	return l.ID
}

// Device is the physical device that owns one or more light services.
type Device struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Metadata *Metadata            `json:"metadata,omitempty"`
	Services []ResourceIdentifier `json:"services,omitempty"`
}

// Group covers both room and zone. They are the same struct but not the same
// idea: a room's Children are devices, a zone's Children are lights.
type Group struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Metadata *Metadata            `json:"metadata,omitempty"`
	Children []ResourceIdentifier `json:"children,omitempty"`
	Services []ResourceIdentifier `json:"services,omitempty"`
}

// Name returns the group's name, falling back to its id.
func (g Group) Name() string {
	if g.Metadata != nil && g.Metadata.Name != "" {
		return g.Metadata.Name
	}
	return g.ID
}

// SmartScene is a 24-hour scene bound to a room or zone. Only its name, group
// and state are ever read. The bridge owns timeslot selection once it is
// active.
type SmartScene struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Metadata *Metadata          `json:"metadata,omitempty"`
	Group    ResourceIdentifier `json:"group"`
	State    string             `json:"state,omitempty"`
}

// Name returns the smart scene's name, falling back to its id.
func (s SmartScene) Name() string {
	if s.Metadata != nil && s.Metadata.Name != "" {
		return s.Metadata.Name
	}
	return s.ID
}

// ZigbeeConnectivity reports whether a device is reachable. Owner points at the
// device, not the light.
type ZigbeeConnectivity struct {
	ID     string             `json:"id"`
	Type   string             `json:"type"`
	Owner  ResourceIdentifier `json:"owner"`
	Status string             `json:"status,omitempty"`
}

// SmartSceneRecall is the only payload we ever write to the bridge.
//
// The recall object accepts nothing but an action. There is no transition
// override, so the fade follows the smart scene's own transition_duration as
// configured in the Hue app.
type SmartSceneRecall struct {
	Recall struct {
		Action string `json:"action"`
	} `json:"recall"`
}

// RecallActivate builds the activate payload.
func RecallActivate() SmartSceneRecall {
	var r SmartSceneRecall
	r.Recall.Action = "activate"
	return r
}

// Envelope is the standard CLIP v2 response wrapper.
type Envelope struct {
	Errors []APIError        `json:"errors"`
	Data   []json.RawMessage `json:"data"`
}

// APIError is a single error entry from the bridge.
type APIError struct {
	Description string `json:"description"`
}

// Event is one element of an SSE data frame.
type Event struct {
	CreationTime string            `json:"creationtime"`
	ID           string            `json:"id"`
	Type         string            `json:"type"` // add | update | delete | error
	Data         []json.RawMessage `json:"data"`
}

// PeekType reports the rtype of a raw resource without fully decoding it.
func PeekType(raw json.RawMessage) string {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.Type
}
