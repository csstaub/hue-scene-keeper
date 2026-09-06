package hue

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BridgeInfo describes a bridge found on the network.
type BridgeInfo struct {
	ID      string `json:"bridgeid"`
	Name    string `json:"name"`
	ModelID string `json:"modelid"`
	Address string `json:"-"`
	Source  string `json:"-"` // "mdns" or "cloud"
}

const (
	// maxCandidates bounds how many addresses we will probe. The mDNS reply
	// is unauthenticated UDP from anyone who can reach our ephemeral port, and
	// a single 9KB datagram can carry ~600 A records; at eight probes in
	// flight and a 5s timeout each, working through them all would stall
	// startup for six minutes on a stranger's say-so.
	maxCandidates = 32
	// maxProbeParallelism keeps that bounded set fast without hammering the
	// network.
	maxProbeParallelism = 8

	mdnsAddr    = "224.0.0.251:5353"
	mdnsService = "_hue._tcp.local"
	dnsTypeA    = 1
	dnsTypePTR  = 12
	dnsTypeSRV  = 33
)

// Discover finds Hue bridges, preferring mDNS and falling back to Signify's
// cloud discovery endpoint. Every candidate address is verified by reading its
// unauthenticated /api/config, which is what keeps loose mDNS parsing from
// offering up arbitrary hosts - though only that they look like a bridge, not
// that they are yours. See parseARecords.
func Discover(ctx context.Context, timeout time.Duration) ([]BridgeInfo, error) {
	addrs, mdnsErr := discoverMDNS(ctx, timeout)
	found, unreachable := probeAll(ctx, addrs, "mdns")

	var cloudErr error
	if len(found) == 0 {
		var cloudAddrs []string
		cloudAddrs, cloudErr = discoverCloud(ctx)
		cloudFound, cloudUnreachable := probeAll(ctx, cloudAddrs, "cloud")
		found = append(found, cloudFound...)
		unreachable = append(unreachable, cloudUnreachable...)
	}

	seen := map[string]BridgeInfo{}
	for _, info := range found {
		seen[info.ID] = info
	}
	out := make([]BridgeInfo, 0, len(seen))
	for _, info := range seen {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	if len(out) > 0 {
		return out, nil
	}

	// Nothing found: say which of the two mechanisms failed and how. Reporting
	// only "no bridge found" hides the common causes - a blocked multicast
	// route, or a bridge that answers discovery but not us.
	var problems []string
	if mdnsErr != nil {
		problems = append(problems, "mdns: "+mdnsErr.Error())
	}
	if cloudErr != nil {
		problems = append(problems, "cloud: "+cloudErr.Error())
	}
	if len(unreachable) > 0 {
		sort.Strings(unreachable)
		problems = append(problems, "found but could not reach: "+strings.Join(unreachable, "; "))
	}
	if len(problems) == 0 {
		return nil, errors.New("no Hue bridge found (try setting bridge.address in the config)")
	}
	return nil, fmt.Errorf("no Hue bridge found - %s", strings.Join(problems, "; "))
}

// probeAll verifies candidate addresses concurrently, returning the confirmed
// bridges and a description of each address that did not answer.
func probeAll(ctx context.Context, addrs []string, source string) ([]BridgeInfo, []string) {
	if len(addrs) > maxCandidates {
		addrs = addrs[:maxCandidates]
	}
	type result struct {
		info BridgeInfo
		err  error
		addr string
	}
	results := make([]result, len(addrs))

	// One client for the whole pass: a transport per candidate would leave up
	// to maxCandidates idle connection pools behind, each holding its sockets
	// open for the transport's idle timeout long after discovery returned.
	client, transport := newProbeClient()
	defer transport.CloseIdleConnections()

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxProbeParallelism)
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			info, err := probe(ctx, client, addr)
			info.Source = source
			results[i] = result{info: info, err: err, addr: addr}
		}(i, addr)
	}
	wg.Wait()

	var found []BridgeInfo
	var unreachable []string
	for _, r := range results {
		if r.err != nil {
			unreachable = append(unreachable, fmt.Sprintf("%s (%v)", r.addr, r.err))
			continue
		}
		found = append(found, r.info)
	}
	return found, unreachable
}

// newProbeClient builds the client used for probing.
//
// Pre-trust: we have no pin for a bridge we have not adopted yet, and
// /api/config exposes nothing sensitive.
func newProbeClient() (*http.Client, *http.Transport) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, transport
}

// Probe reads a bridge's unauthenticated config to confirm it really is one.
func Probe(ctx context.Context, addr string) (BridgeInfo, error) {
	client, transport := newProbeClient()
	defer transport.CloseIdleConnections()
	return probe(ctx, client, addr)
}

func probe(ctx context.Context, client *http.Client, addr string) (BridgeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+hostPort(addr)+"/api/config", nil)
	if err != nil {
		return BridgeInfo{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return BridgeInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return BridgeInfo{}, fmt.Errorf("probe %s: %s", addr, resp.Status)
	}
	var info BridgeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return BridgeInfo{}, err
	}
	if info.ID == "" {
		return BridgeInfo{}, fmt.Errorf("%s is not a Hue bridge", addr)
	}
	info.Address = addr
	return info, nil
}

func discoverCloud(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://discovery.meethue.com/", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery.meethue.com: %s", resp.Status)
	}
	var entries []struct {
		ID   string `json:"id"`
		IP   string `json:"internalipaddress"`
		Port int    `json:"port"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&entries); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		// Validate before splicing into a request URL.
		if net.ParseIP(e.IP) == nil {
			continue
		}
		if e.Port > 0 && e.Port != 443 && e.Port <= 65535 {
			out = append(out, net.JoinHostPort(e.IP, strconv.Itoa(e.Port)))
			continue
		}
		out = append(out, hostPort(e.IP))
	}
	return out, nil
}

// discoverMDNS sends a one-shot multicast PTR query for _hue._tcp.local and
// gathers the A records from whatever answers it.
func discoverMDNS(ctx context.Context, timeout time.Duration) ([]string, error) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, fmt.Errorf("mdns socket: %w", err)
	}
	defer func() { _ = conn.Close() }()

	dst, err := net.ResolveUDPAddr("udp4", mdnsAddr)
	if err != nil {
		return nil, err
	}
	query, err := buildQuery(mdnsService, dnsTypePTR)
	if err != nil {
		return nil, err
	}
	if _, err := conn.WriteToUDP(query, dst); err != nil {
		return nil, fmt.Errorf("mdns send: %w", err)
	}

	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return collectMDNS(ctx, conn, deadline)
}

// collectMDNS reads answers off conn until the listen window closes, the
// candidate budget fills, or ctx ends.
//
// It returns whatever it collected alongside any error, because a partial list
// is still worth probing. Reaching the deadline is not an error - that is the
// normal way this finishes.
func collectMDNS(ctx context.Context, conn *net.UDPConn, deadline time.Time) ([]string, error) {
	_ = conn.SetReadDeadline(deadline)

	// A deadline covers a context that expires, but not one that is cancelled:
	// without this, Ctrl-C during `discover` sat in ReadFromUDP until the full
	// listen window had run out. Bringing the deadline forward to now is what
	// wakes the read; the loop then sees ctx.Err() and stops.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-stop:
		}
	}()

	var readErr error
	found := map[string]struct{}{}
	buf := make([]byte, 9000)
	for len(found) < maxCandidates {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Three ways out, and they used to be indistinguishable: the
			// listen window closing (the normal one), the caller giving up,
			// and the socket actually failing. Reporting the last as a silent
			// stop left Discover saying "no Hue bridge found" with no hint
			// that the network layer had refused to play.
			var ne net.Error
			if ctx.Err() != nil {
				readErr = ctx.Err()
			} else if !errors.As(err, &ne) || !ne.Timeout() {
				readErr = fmt.Errorf("mdns read: %w", err)
			}
			break
		}
		for _, ip := range parseARecords(buf[:n]) {
			if len(found) >= maxCandidates {
				break
			}
			found[ip] = struct{}{}
		}
	}
	out := make([]string, 0, len(found))
	for ip := range found {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out, readErr
}

func buildQuery(name string, qtype uint16) ([]byte, error) {
	var b bytes.Buffer
	hdr := make([]byte, 12)
	binary.BigEndian.PutUint16(hdr[4:6], 1) // one question
	b.Write(hdr)
	if err := encodeName(&b, name); err != nil {
		return nil, err
	}
	var tail [4]byte
	binary.BigEndian.PutUint16(tail[0:2], qtype)
	// QCLASS IN with the unicast-response bit set, so replies come straight
	// back to our ephemeral port rather than to the multicast group.
	binary.BigEndian.PutUint16(tail[2:4], 0x8001)
	b.Write(tail[:])
	return b.Bytes(), nil
}

func encodeName(b *bytes.Buffer, name string) error {
	for _, label := range strings.Split(strings.TrimSuffix(name, "."), ".") {
		if len(label) == 0 || len(label) > 63 {
			return fmt.Errorf("invalid dns label %q", label)
		}
		b.WriteByte(byte(len(label)))
		b.WriteString(label)
	}
	b.WriteByte(0)
	return nil
}

// readName decodes the name at off and returns it lower-cased and dotted,
// along with the offset just past the name as it appears at off - which for a
// compressed name is two bytes on, not wherever the pointer led.
func readName(msg []byte, off int) (string, int, error) {
	var name strings.Builder
	next := -1
	cur := off
	for hops := 0; ; {
		if cur >= len(msg) {
			return "", 0, errors.New("truncated name")
		}
		l := int(msg[cur])
		switch {
		case l == 0:
			if next < 0 {
				next = cur + 1
			}
			return strings.ToLower(strings.TrimSuffix(name.String(), ".")), next, nil
		case l&0xC0 == 0xC0:
			if cur+1 >= len(msg) {
				return "", 0, errors.New("truncated compression pointer")
			}
			ptr := int(binary.BigEndian.Uint16(msg[cur:cur+2]) & 0x3FFF)
			if next < 0 {
				next = cur + 2
			}
			// Pointers must go backwards. Insisting on that, and on a hop
			// budget, is what keeps a hostile datagram from looping us.
			hops++
			if ptr >= cur || hops > 16 {
				return "", 0, errors.New("bad compression pointer")
			}
			cur = ptr
		case l > 63:
			return "", 0, fmt.Errorf("invalid label length %d", l)
		default:
			if cur+1+l > len(msg) {
				return "", 0, errors.New("truncated label")
			}
			name.Write(msg[cur+1 : cur+1+l])
			name.WriteByte('.')
			cur += 1 + l
		}
	}
}

// parseARecords walks every section of a DNS message and returns the IPv4
// addresses of all A records it contains.
//
// It drops anything that is not a response, and anything whose question
// section - which a responder answering our unicast query is meant to echo
// back - asks about a service other than ours. A reply carrying no question at
// all is still accepted: a responder is entitled to send an ordinary
// multicast-shaped answer, and turning those away would break discovery
// against bridges that do.
//
// Past that point it still over-collects deliberately: A records from the
// authority and additional sections are taken without checking which name they
// belong to, because that is where a bridge's address usually rides. Probe is
// what turns a candidate into a bridge. Note what Probe cannot do - it
// confirms the peer answers /api/config with a bridgeid, not that it is *your*
// bridge, so a LAN host that fakes one is an adoption candidate. That is
// inherent to unauthenticated mDNS; the `discover` command prints the id and
// name so the user can check them against the sticker.
func parseARecords(msg []byte) []string {
	if len(msg) < 12 {
		return nil
	}
	// QR clear means someone else's query, not an answer to ours.
	if msg[2]&0x80 == 0 {
		return nil
	}
	qd := int(binary.BigEndian.Uint16(msg[4:6]))
	counts := []int{
		int(binary.BigEndian.Uint16(msg[6:8])),   // answers
		int(binary.BigEndian.Uint16(msg[8:10])),  // authority
		int(binary.BigEndian.Uint16(msg[10:12])), // additional
	}

	off := 12
	var err error
	for i := 0; i < qd; i++ {
		var qname string
		if qname, off, err = readName(msg, off); err != nil {
			return nil
		}
		if off+4 > len(msg) {
			return nil
		}
		qtype := binary.BigEndian.Uint16(msg[off : off+2])
		off += 4 // qtype + qclass
		if qname != mdnsService || qtype != dnsTypePTR {
			return nil
		}
	}

	var out []string
	total := counts[0] + counts[1] + counts[2]
	for i := 0; i < total; i++ {
		if _, off, err = readName(msg, off); err != nil {
			return out
		}
		if off+10 > len(msg) {
			return out
		}
		rtype := binary.BigEndian.Uint16(msg[off : off+2])
		rdlen := int(binary.BigEndian.Uint16(msg[off+8 : off+10]))
		off += 10
		if off+rdlen > len(msg) {
			return out
		}
		if rtype == dnsTypeA && rdlen == 4 {
			out = append(out, net.IP(msg[off:off+4]).String())
		}
		off += rdlen
	}
	return out
}
