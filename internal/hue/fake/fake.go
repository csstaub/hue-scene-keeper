// Package fake implements enough of a Hue bridge to exercise the daemon
// end-to-end in tests. The handful of resource endpoints the daemon reads, the
// smart scene recall it writes, and a scriptable SSE event stream.
package fake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/csstaub/hue-scene-keeper/internal/hue"
)

// Bridge is a fake Hue bridge backed by an httptest TLS server.
type Bridge struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	devices map[string]hue.Device
	lights  map[string]hue.Light
	rooms   map[string]hue.Group
	zones   map[string]hue.Group
	scenes  map[string]hue.SmartScene
	conns   map[string]hue.ZigbeeConnectivity
	recalls []string
	// attempts counts every recall request the bridge answered, refusals
	// included. recalls counts only the ones it accepted.
	attempts int
	// lightGets counts reads of a single light by id, the request a
	// power-restore lookup makes. The bulk GET the resync does is not
	// counted. What a test wants to know is how many lamps a lookup spent a
	// rate-limiter token on.
	lightGets int
	subs      map[*subscriber]struct{}
	keepalive time.Duration

	// echoes tracks the recall-echo goroutines, which are started after the
	// response is written and so outlive the request. Without waiting for
	// them, one can log after the test finished and panic the whole binary.
	echoes sync.WaitGroup

	// failures collects problems noticed on goroutines other than the test's.
	// t.Fatalf from those calls runtime.Goexit on the wrong goroutine, which
	// does not stop the test. They are drained in Cleanup instead.
	failures []string
	nextID   int
	// failRecalls makes the next N recalls return failStatus, for exercising
	// the daemon's error handling.
	failRecalls int
	failStatus  int
	// recallDelay holds each recall response open for this long, for
	// exercising the client's per-request timeout.
	recallDelay time.Duration

	// EchoOnRecall reproduces the behavior that makes loop suppression
	// necessary. A recall turns on every light in the group, and each of
	// those lights reports itself as newly on.
	EchoOnRecall bool
}

// subscriber is one open event-stream connection.
type subscriber struct {
	frames chan string
	quit   chan struct{}
}

// NewBridge starts a fake bridge and registers cleanup.
func NewBridge(t *testing.T) *Bridge {
	t.Helper()
	b := &Bridge{
		t:       t,
		devices: map[string]hue.Device{},
		lights:  map[string]hue.Light{},
		rooms:   map[string]hue.Group{},
		zones:   map[string]hue.Group{},
		scenes:  map[string]hue.SmartScene{},
		conns:   map[string]hue.ZigbeeConnectivity{},
		subs:    map[*subscriber]struct{}{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/eventstream/clip/v2", b.requireKey(b.handleStream))
	mux.HandleFunc("/clip/v2/resource/", b.requireKey(b.handleResource))
	b.server = httptest.NewTLSServer(mux)
	t.Cleanup(func() {
		b.server.Close()
		// Close does not wait for the echo goroutines. They are spawned after
		// the response is written, so they are not "in flight" as far as the
		// server is concerned.
		b.echoes.Wait()
		b.mu.Lock()
		failures := b.failures
		b.mu.Unlock()
		for _, f := range failures {
			t.Error(f)
		}
	})
	return b
}

// fail records a problem noticed on a goroutine that is not the test's.
//
// t.Fatalf from such a goroutine calls runtime.Goexit on the wrong one, which
// does not stop the test and can leave it reporting a confusing downstream
// failure instead. Recording and draining in Cleanup reports it against the
// right test, in the right place.
func (b *Bridge) fail(format string, args ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = append(b.failures, fmt.Sprintf(format, args...))
}

// Client returns a client pointed at the fake, with pinning disabled and the
// rate limiter effectively off so tests do not wait on it.
func (b *Bridge) Client() *hue.Client {
	return b.ClientWithTimeout(0)
}

// ClientWithTimeout builds a client with a non-default per-request timeout.
// Pair it with DelayRecalls to exercise what the daemon does with a bridge
// that accepts a request and then never answers.
func (b *Bridge) ClientWithTimeout(d time.Duration) *hue.Client {
	return hue.New(hue.Options{
		BaseURL:           b.server.URL,
		AppKey:            "test-key",
		Insecure:          true,
		RequestsPerSecond: 10000,
		RequestTimeout:    d,
	})
}

// URL returns the fake bridge's base URL.
func (b *Bridge) URL() string { return b.server.URL }

// --- topology helpers -------------------------------------------------------

// AddRoom creates a room containing one device per light name, each device
// exposing a single light service. That mirrors how real Hue lamps are
// modeled.
func (b *Bridge) AddRoom(roomID, name string, lightNames ...string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	room := hue.Group{ID: roomID, Type: hue.TypeRoom, Metadata: &hue.Metadata{Name: name}}
	var lightIDs []string
	for i, lightName := range lightNames {
		deviceID := fmt.Sprintf("%s-dev-%d", roomID, i)
		lightID := fmt.Sprintf("%s-light-%d", roomID, i)
		connID := fmt.Sprintf("%s-conn-%d", roomID, i)

		b.devices[deviceID] = hue.Device{
			ID: deviceID, Type: hue.TypeDevice,
			Metadata: &hue.Metadata{Name: lightName},
			Services: []hue.ResourceIdentifier{{RID: lightID, RType: hue.TypeLight}},
		}
		b.lights[lightID] = hue.Light{
			ID: lightID, Type: hue.TypeLight,
			Owner:    hue.ResourceIdentifier{RID: deviceID, RType: hue.TypeDevice},
			Metadata: &hue.Metadata{Name: lightName},
			On:       &hue.On{On: false},
		}
		b.conns[connID] = hue.ZigbeeConnectivity{
			ID: connID, Type: hue.TypeZigbeeConnectivity,
			Owner:  hue.ResourceIdentifier{RID: deviceID, RType: hue.TypeDevice},
			Status: hue.StatusConnected,
		}
		room.Children = append(room.Children,
			hue.ResourceIdentifier{RID: deviceID, RType: hue.TypeDevice})
		lightIDs = append(lightIDs, lightID)
	}
	b.rooms[roomID] = room
	return lightIDs
}

// AddZone creates a zone referencing existing lights directly, as real zones do.
func (b *Bridge) AddZone(zoneID, name string, lightIDs ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	zone := hue.Group{ID: zoneID, Type: hue.TypeZone, Metadata: &hue.Metadata{Name: name}}
	for _, id := range lightIDs {
		zone.Children = append(zone.Children, hue.ResourceIdentifier{RID: id, RType: hue.TypeLight})
	}
	b.zones[zoneID] = zone
}

// AddSmartScene binds a smart scene to a group.
func (b *Bridge) AddSmartScene(sceneID, name, groupID, groupType string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scenes[sceneID] = hue.SmartScene{
		ID: sceneID, Type: hue.TypeSmartScene,
		Metadata: &hue.Metadata{Name: name},
		Group:    hue.ResourceIdentifier{RID: groupID, RType: groupType},
		State:    hue.SmartSceneInactive,
	}
}

// ConnectivityIDFor returns the connectivity service id of a light's device.
func (b *Bridge) ConnectivityIDFor(lightID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	owner := b.lights[lightID].Owner.RID
	for id, c := range b.conns {
		if c.Owner.RID == owner {
			return id
		}
	}
	return ""
}

// --- state changes ----------------------------------------------------------

// SwitchLight sets a light's state and publishes the update event a real
// bridge would send.
func (b *Bridge) SwitchLight(lightID string, on bool) {
	b.mu.Lock()
	light, ok := b.lights[lightID]
	if !ok {
		b.mu.Unlock()
		b.fail("fake bridge: unknown light %q", lightID)
		return
	}
	light.On = &hue.On{On: on}
	b.lights[lightID] = light
	b.mu.Unlock()

	// Real update events are partial. Just id, type, owner and the changed
	// field.
	b.Publish("update", map[string]any{
		"id": lightID, "type": hue.TypeLight,
		"owner": light.Owner,
		"on":    map[string]bool{"on": on},
	})
}

// SetConnectivity changes a device's radio status and publishes the event.
// Use it to simulate a lamp losing and regaining mains power.
func (b *Bridge) SetConnectivity(connID, status string) {
	b.mu.Lock()
	conn, ok := b.conns[connID]
	if !ok {
		b.mu.Unlock()
		b.fail("fake bridge: unknown connectivity service %q", connID)
		return
	}
	conn.Status = status
	b.conns[connID] = conn
	b.mu.Unlock()

	b.Publish("update", map[string]any{
		"id": connID, "type": hue.TypeZigbeeConnectivity,
		"owner": conn.Owner, "status": status,
	})
}

// SetLightStateSilently changes a light without publishing an event. It models
// a change that happened while the daemon was disconnected.
func (b *Bridge) SetLightStateSilently(lightID string, on bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	light := b.lights[lightID]
	light.On = &hue.On{On: on}
	b.lights[lightID] = light
}

// Publish sends one resource change down the event stream.
func (b *Bridge) Publish(action string, resources ...map[string]any) {
	data := make([]json.RawMessage, 0, len(resources))
	for _, r := range resources {
		raw, err := json.Marshal(r)
		if err != nil {
			b.fail("fake bridge: marshal event: %v", err)
			continue
		}
		data = append(data, raw)
	}
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	b.mu.Unlock()

	payload, err := json.Marshal([]hue.Event{{
		CreationTime: time.Now().UTC().Format(time.RFC3339),
		ID:           fmt.Sprint(id),
		Type:         action,
		Data:         data,
	}})
	if err != nil {
		b.fail("fake bridge: marshal frame: %v", err)
		return
	}
	b.broadcast(fmt.Sprintf("id: %d\ndata: %s\n\n", id, payload))
}

// PublishRaw sends a verbatim SSE frame, for testing the framing itself.
func (b *Bridge) PublishRaw(frame string) { b.broadcast(frame) }

func (b *Bridge) broadcast(frame string) {
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	// One timer for the whole fan-out, reset per subscriber. time.After here
	// would leave a live two-second timer behind for every subscriber of
	// every frame, which a test that publishes in a loop accumulates. No
	// drain after Stop, because since Go 1.23 a reset timer cannot deliver a
	// stale value.
	stuck := time.NewTimer(2 * time.Second)
	defer stuck.Stop()
	for _, s := range subs {
		stuck.Stop()
		stuck.Reset(2 * time.Second)
		select {
		case s.frames <- frame:
		case <-s.quit:
		case <-stuck.C:
			b.fail("fake bridge: subscriber did not consume event frame")
		}
	}
}

// FailNextRecalls makes the next n recall requests return 503, simulating a
// busy or rebooting bridge.
func (b *Bridge) FailNextRecalls(n int) {
	b.FailNextRecallsWith(n, http.StatusServiceUnavailable)
}

// FailNextRecallsWith makes the next n recall requests return status. Use it
// to tell a transient refusal (429, 503) apart from a permanent one (404),
// which the daemon must not retry.
func (b *Bridge) FailNextRecallsWith(n, status int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failRecalls = n
	b.failStatus = status
}

// DelayRecalls holds every recall response open for d before answering,
// simulating a bridge slow enough to hit the client's request timeout.
func (b *Bridge) DelayRecalls(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recallDelay = d
}

// TouchLight publishes a light update that changes brightness and nothing else.
// No "on" field at all, which is what a real bridge sends when something writes
// to a light that is already on. Such an event carries no off->on edge and so
// triggers nothing by itself.
func (b *Bridge) TouchLight(lightID string, brightness float64) {
	b.mu.Lock()
	light, ok := b.lights[lightID]
	b.mu.Unlock()
	if !ok {
		b.fail("fake bridge: unknown light %q", lightID)
		return
	}
	b.Publish("update", map[string]any{
		"id": lightID, "type": hue.TypeLight,
		"owner":   light.Owner,
		"dimming": map[string]float64{"brightness": brightness},
	})
}

// DropStreams closes every open event-stream connection, simulating the bridge
// dropping the connection or a network blip.
func (b *Bridge) DropStreams() {
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = map[*subscriber]struct{}{}
	b.mu.Unlock()
	for _, s := range subs {
		close(s.quit)
	}
}

// SubscriberCount reports how many event-stream connections are open.
func (b *Bridge) SubscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// Attempts returns how many recall requests the bridge has answered,
// including the ones it refused. Use it to tell "retried" from "gave up".
func (b *Bridge) Attempts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts
}

// LightGets reports how many times a single light has been read by id, the
// request a power-restore lookup makes. The resync's bulk read of every light
// does not count toward it.
func (b *Bridge) LightGets() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lightGets
}

// Recalls returns the smart scene ids recalled so far, in order.
func (b *Bridge) Recalls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.recalls...)
}

// WaitForSubscriber blocks until an event stream client has connected.
func (b *Bridge) WaitForSubscriber(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		n := len(b.subs)
		b.mu.Unlock()
		if n > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// --- HTTP handlers ----------------------------------------------------------

// knownTypes are the resource types the fake serves. Anything else is a 404, as
// on a real bridge. Answering 200 with an empty list would let a typo'd or
// renamed rtype read as "this home has no lights" instead of as an error.
var knownTypes = map[string]bool{
	hue.TypeDevice:             true,
	hue.TypeLight:              true,
	hue.TypeRoom:               true,
	hue.TypeZone:               true,
	hue.TypeSmartScene:         true,
	hue.TypeZigbeeConnectivity: true,
}

func (b *Bridge) handleResource(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/clip/v2/resource/")
	parts := strings.Split(path, "/")
	rtype := parts[0]

	if !knownTypes[rtype] {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `{"errors":[{"description":"unknown resource type %s"}],"data":[]}`, rtype)
		return
	}

	if r.Method == http.MethodPut {
		b.handlePut(w, r, rtype, parts)
		return
	}
	if len(parts) == 2 && parts[1] != "" {
		if rtype == hue.TypeLight {
			b.mu.Lock()
			b.lightGets++
			b.mu.Unlock()
		}
		b.writeEnvelope(w, b.collect(rtype, parts[1]))
		return
	}
	b.writeEnvelope(w, b.collect(rtype, ""))
}

func (b *Bridge) handlePut(w http.ResponseWriter, r *http.Request, rtype string, parts []string) {
	if rtype != hue.TypeSmartScene || len(parts) < 2 {
		http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		return
	}
	var body hue.SmartSceneRecall
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	sceneID := parts[1]

	b.mu.Lock()
	scene, ok := b.scenes[sceneID]
	if !ok {
		b.mu.Unlock()
		http.Error(w, "no such smart scene", http.StatusNotFound)
		return
	}
	// Counted on arrival, before any refusal or stalling, so a test can see
	// that a request reached the bridge even when the client gives up on the
	// answer.
	b.attempts++
	if b.failRecalls > 0 {
		b.failRecalls--
		status := b.failStatus
		b.mu.Unlock()
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if d := b.recallDelay; d > 0 {
		b.mu.Unlock()
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
		b.mu.Lock()
	}
	b.recalls = append(b.recalls, sceneID)
	if body.Recall.Action == "activate" {
		scene.State = hue.SmartSceneActive
		b.scenes[sceneID] = scene
	}
	echo := b.EchoOnRecall
	groupID := scene.Group.RID
	b.mu.Unlock()

	b.writeEnvelope(w, []any{map[string]any{"rid": sceneID, "rtype": hue.TypeSmartScene}})

	if echo && body.Recall.Action == "activate" {
		// A real bridge turns on every light in the group, and each reports
		// itself as newly on. This is the feedback the daemon must not chase.
		b.echoes.Add(1)
		go func() {
			defer b.echoes.Done()
			b.echoGroupOn(groupID)
		}()
	}
}

func (b *Bridge) echoGroupOn(groupID string) {
	b.mu.Lock()
	var lightIDs []string
	if room, ok := b.rooms[groupID]; ok {
		for _, child := range room.Children {
			for _, svc := range b.devices[child.RID].Services {
				if svc.RType == hue.TypeLight {
					lightIDs = append(lightIDs, svc.RID)
				}
			}
		}
	} else if zone, ok := b.zones[groupID]; ok {
		for _, child := range zone.Children {
			lightIDs = append(lightIDs, child.RID)
		}
	}
	b.mu.Unlock()

	for _, id := range lightIDs {
		b.SwitchLight(id, true)
	}
}

// collect returns the resources of a type, optionally filtered to one id.
func (b *Bridge) collect(rtype, id string) []any {
	b.mu.Lock()
	defer b.mu.Unlock()

	var out []any
	add := func(key string, v any) {
		if id == "" || id == key {
			out = append(out, v)
		}
	}
	switch rtype {
	case hue.TypeDevice:
		for k, v := range b.devices {
			add(k, v)
		}
	case hue.TypeLight:
		for k, v := range b.lights {
			add(k, v)
		}
	case hue.TypeRoom:
		for k, v := range b.rooms {
			add(k, v)
		}
	case hue.TypeZone:
		for k, v := range b.zones {
			add(k, v)
		}
	case hue.TypeSmartScene:
		for k, v := range b.scenes {
			add(k, v)
		}
	case hue.TypeZigbeeConnectivity:
		for k, v := range b.conns {
			add(k, v)
		}
	}
	return out
}

func (b *Bridge) writeEnvelope(w http.ResponseWriter, data []any) {
	w.Header().Set("Content-Type", "application/json")
	raw := make([]json.RawMessage, 0, len(data))
	for _, d := range data {
		enc, err := json.Marshal(d)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		raw = append(raw, enc)
	}
	_ = json.NewEncoder(w).Encode(hue.Envelope{Errors: []hue.APIError{}, Data: raw})
}

func (b *Bridge) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	sub := &subscriber{frames: make(chan string, 64), quit: make(chan struct{})}
	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.subs, sub)
		b.mu.Unlock()
	}()

	// The real bridge sends this at connect and then, on a live but quiet
	// connection, nothing at all for minutes.
	if _, err := fmt.Fprint(w, ": hi\n\n"); err != nil {
		return
	}
	flusher.Flush()

	// Keepalives are opt-in and off by default, because the real bridge does
	// not send them. A fake that chatters every 200ms means no test ever sees
	// an idle stream, so nothing exercises the watchdog or anything else that
	// has to tolerate multi-minute silence.
	b.mu.Lock()
	period := b.keepalive
	b.mu.Unlock()

	var keepaliveC <-chan time.Time
	if period > 0 {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		keepaliveC = ticker.C
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.quit:
			return
		case frame := <-sub.frames:
			if _, err := fmt.Fprint(w, frame); err != nil {
				return
			}
			flusher.Flush()
		case <-keepaliveC:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// Keepalive makes the fake send SSE comment frames every d. It is off by
// default. The real bridge does not keepalive, and a stream that is never idle
// hides every bug in the code that copes with silence.
func (b *Bridge) Keepalive(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keepalive = d
}

// requireKey rejects a request without the application key, as the bridge does.
// Without this a regression that stopped sending the header would pass every
// test here and fail against every real bridge.
func (b *Bridge) requireKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("hue-application-key") != "test-key" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `{"errors":[{"description":"unauthorized user"}],"data":[]}`)
			return
		}
		next(w, r)
	}
}
