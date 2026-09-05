package hue

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestManualDiscovery exercises discovery against the real network. It is
// skipped unless HUE_MANUAL=1, so it never runs in CI.
func TestManualDiscovery(t *testing.T) {
	if os.Getenv("HUE_MANUAL") == "" {
		t.Skip("set HUE_MANUAL=1 to run against a real bridge")
	}
	ctx := context.Background()

	if addr := os.Getenv("HUE_ADDRESS"); addr != "" {
		info, err := Probe(ctx, addr)
		t.Logf("Probe(%s) -> %+v err=%v", addr, info, err)
	}

	addrs, err := discoverMDNS(ctx, 3*time.Second)
	t.Logf("mDNS -> %v err=%v", addrs, err)

	cloud, err := discoverCloud(ctx)
	t.Logf("cloud -> %v err=%v", cloud, err)

	bridges, err := Discover(ctx, 3*time.Second)
	t.Logf("Discover -> %+v err=%v", bridges, err)
}
