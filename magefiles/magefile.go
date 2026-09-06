//go:build mage

// The mage build tag above keeps this directory out of `go build ./...` and
// `go vet ./...`: mage generates the main function at compile time, so without
// the tag this would be a main package that has none.

// Build targets for hue-scene-keeper. Run `go tool mage -l` to list them.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	binary  = "hue-scene-keeper"
	mainPkg = "./cmd/hue-scene-keeper"
	distDir = "dist"

	// exampleConfig is the commented reference config: the macOS installer
	// ships it as documentation, and installConfig writes it to the path the
	// systemd unit reads.
	exampleConfig = "config.example.yaml"
	// unitFile is the systemd unit. Its ExecStart is the single source of the
	// paths a Linux install uses, and installConfig reads one of them back out
	// of it rather than repeating it here.
	unitFile = "deploy/hue-scene-keeper.service"

	// bundleID is the launchd label, the pkg identifier, and the receipt name.
	bundleID = "dev.staub.hue-scene-keeper"
	// buildDir is scratch space for the installer payload; not shipped.
	buildDir = "build"
)

// Go groups everything driven by the Go toolchain: go:build, go:test, and so on.
type Go mg.Namespace

// Pkg groups the macOS installer: pkg:build, pkg:install, pkg:uninstall.
type Pkg mg.Namespace

// Platforms worth shipping: this Mac, a Linux server, and a Raspberry Pi.
var platforms = []struct{ goos, goarch string }{
	{"darwin", "arm64"},
	{"darwin", "amd64"},
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"linux", "arm"},
}

// Default is the target mage runs when given no arguments.
var Default = All

// version resolves once per run: $VERSION wins, then git, then "dev".
var version = sync.OnceValue(func() string {
	if v := os.Getenv("VERSION"); v != "" {
		return v
	}
	// Discard git's stderr: in a repo with no commits it prints "fatal: bad
	// revision 'HEAD'", which is not an error worth showing. The Makefile
	// suppressed it with 2>/dev/null.
	var out bytes.Buffer
	if _, err := sh.Exec(nil, &out, io.Discard, "git", "describe", "--tags", "--always", "--dirty"); err == nil {
		if v := strings.TrimSpace(out.String()); v != "" {
			return v
		}
	}
	return "dev"
})

func ldflags() string {
	return fmt.Sprintf("-s -w -X main.version=%s", version())
}

// All vets, tests, then builds. The default target.
func All() { mg.SerialDeps(Go.Vet, Go.Test, Go.Build) }

// Clean removes the binary and every build output directory. It spans both
// namespaces, so it stays at the top level.
func Clean() error {
	for _, path := range []string{binary, distDir, buildDir} {
		if err := sh.Rm(path); err != nil {
			return err
		}
	}
	return nil
}

// InstallConfig writes the example config to the path the systemd unit passes
// to --config, stamped with the version and the moment it was installed. It
// belongs to no toolchain, so it stays at the top level beside Clean.
//
// The unit names that file explicitly, and an explicitly-given --config that
// does not exist is a fatal error by design - it is what catches a typo in a
// hand-typed path. So a systemd install has to create the file, and this is
// that step. What it writes selects nothing: the example is entirely comments,
// so every default stays the daemon's to change, including in a later version.
//
// The destination is read back out of the unit rather than written here a
// second time, so the two cannot drift - which is the bug this target exists
// to close. An existing file is left exactly as it is: it is the operator's,
// and an upgrade must never rewrite it.
//
// Run it as yourself and not under sudo, for the same reason as go:install:
// mage compiles this file, and `sudo -E` preserving HOME leaves root-owned
// entries in your build cache that every later build of yours then fails on.
// Only the two install calls are elevated.
func InstallConfig() error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("installConfig writes the config file the systemd unit reads; this is %s", runtime.GOOS)
	}
	dest, err := unitConfigPath()
	if err != nil {
		return err
	}
	return installConfigTo(dest)
}

// installConfigTo is the whole of InstallConfig bar the platform check and
// working out where the file goes. It is split off so the writing half can be
// run against a temporary destination on a machine that is not the target one.
func installConfigTo(dest string) error {
	switch _, err := os.Stat(dest); {
	case err == nil:
		fmt.Printf("%s is already there, left unchanged\n", dest)
		return nil
	case !os.IsNotExist(err):
		return err
	}

	body, err := stampedExample(version(), time.Now().UTC())
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "hue-scene-keeper-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Guarded rather than unconditional: `install -d` on a directory that
	// already exists rewrites its mode, and systemd's own ConfigurationDirectory
	// may have made this one already.
	dir := filepath.Dir(dest)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := elevated("install", "-d", "-m0755", dir); err != nil {
			return err
		}
	}
	if err := elevated("install", "-m0644", tmp.Name(), dest); err != nil {
		return err
	}
	fmt.Printf("wrote %s - every key commented out, so it is the defaults\n", dest)
	return nil
}

// configFlag matches the unit's `--config <path>`, in either the spaced or the
// = form systemd accepts.
var configFlag = regexp.MustCompile(`--config[=\s]+(\S+)`)

// unitConfigPath reads the --config path out of the unit's ExecStart. Comment
// lines are dropped first: the unit's header documents the install, so a
// --config in the prose would otherwise be found before the real one.
func unitConfigPath() (string, error) {
	raw, err := os.ReadFile(unitFile)
	if err != nil {
		return "", err
	}
	var settings []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			settings = append(settings, line)
		}
	}
	m := configFlag.FindStringSubmatch(strings.Join(settings, "\n"))
	if m == nil {
		return "", fmt.Errorf("%s: no --config path in ExecStart", unitFile)
	}
	return m[1], nil
}

// stampedExample returns the example config with two lines of provenance on
// top: which version wrote it and when. The file is the operator's from then
// on, and nothing this project ships ever reads the stamp back - it is there
// for the person who opens the file in two years and wants to know where it
// came from and how old its comments are.
func stampedExample(version string, at time.Time) ([]byte, error) {
	raw, err := os.ReadFile(exampleConfig)
	if err != nil {
		return nil, err
	}
	header := fmt.Sprintf("# Installed by hue-scene-keeper %s on %s.\n"+
		"# Written once; no upgrade rewrites it. Edit it freely.\n\n",
		version, at.Format(time.RFC3339))
	return append([]byte(header), raw...), nil
}

// --- go: the Go toolchain --------------------------------------------------

// Build compiles the binary for this machine. CGO_ENABLED=0 is not an
// optimization: it is what makes this binary the same shape as the ones
// buildTo cross-compiles. A cgo build resolves hostnames through glibc's
// getaddrinfo, which opens an AF_NETLINK socket that the systemd unit's
// RestrictAddressFamilies does not allow, so cloud discovery would fail under
// the unit on a host where DNS demonstrably works.
func (Go) Build() error {
	env := map[string]string{"CGO_ENABLED": "0"}
	return sh.RunWithV(env, "go", "build", "-ldflags", ldflags(), "-o", binary, mainPkg)
}

// Test runs the test suite.
func (Go) Test() error { return sh.RunV("go", "test", "./...") }

// Race runs the test suite under the race detector.
func (Go) Race() error { return sh.RunV("go", "test", "-race", "./...") }

// Vet runs go vet over every package.
func (Go) Vet() error { return sh.RunV("go", "vet", "./...") }

// Fmt rewrites every Go file with gofmt.
func (Go) Fmt() error { return sh.RunV("gofmt", "-l", "-w", ".") }

// Lint runs golangci-lint. Its dependency tree lives in lint.mod so that the
// module's own go.mod stays small and keeps its lower Go floor.
func (Go) Lint() error {
	return sh.RunV("go", "tool", "-modfile=lint.mod", "golangci-lint", "run")
}

// Install builds the binary and installs it into /usr/local/bin. The compile
// stays unprivileged and only the copy is elevated, so this target is run as
// yourself: under `sudo go tool mage go:install` the dependency on Go.Build
// re-runs the compiler as root, and with -E preserving HOME that leaves
// root-owned entries in your build and module caches which every later
// non-root build then fails on.
func (Go) Install() error {
	mg.Deps(Go.Build)
	dir := "/usr/local/bin"
	// -d first: install refuses to write into a directory that does not exist,
	// and on a fresh machine /usr/local/bin often does not.
	if err := elevated("install", "-d", "-m0755", dir); err != nil {
		return err
	}
	return elevated("install", "-m0755", binary, filepath.Join(dir, binary))
}

// elevated runs one command through sudo unless we are already root, or there
// is no sudo to reach for. internal/service picks up sudo the same way.
func elevated(args ...string) error {
	if os.Geteuid() != 0 {
		if sudo, err := exec.LookPath("sudo"); err == nil {
			args = append([]string{sudo}, args...)
		}
	}
	return sh.RunV(args[0], args[1:]...)
}

// Dist cross-compiles a static binary for every target platform.
func (Go) Dist() error {
	if err := sh.Rm(distDir); err != nil {
		return err
	}
	if err := os.MkdirAll(distDir, 0o755); err != nil {
		return err
	}
	// mg.Deps runs these concurrently; the Makefile's loop was serial.
	targets := make([]any, 0, len(platforms))
	for _, p := range platforms {
		targets = append(targets, mg.F(buildPlatform, p.goos, p.goarch))
	}
	mg.Deps(targets...)
	return nil
}

// buildPlatform builds one static binary for goos/goarch into dist/.
func buildPlatform(goos, goarch string) error {
	return buildTo(goos, goarch, filepath.Join(distDir, fmt.Sprintf("%s-%s-%s", binary, goos, goarch)))
}

// buildTo builds one static binary for goos/goarch at an explicit path.
func buildTo(goos, goarch, out string) error {
	fmt.Println("building", out)
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	env := map[string]string{"CGO_ENABLED": "0", "GOOS": goos, "GOARCH": goarch}
	return sh.RunWith(env, "go", "build", "-trimpath", "-ldflags", ldflags(), "-o", out, mainPkg)
}

// --- pkg: the macOS installer ----------------------------------------------

// pkgPath is where the finished installer lands.
func pkgPath() string {
	return filepath.Join(distDir, fmt.Sprintf("%s-%s.pkg", binary, pkgVersion()))
}

var numericVersion = regexp.MustCompile(`^[0-9]+(\.[0-9]+)*$`)

// pkgVersion reduces the git description to something pkgbuild can order for
// upgrade comparisons: v0.2.1-4-gabc1234-dirty becomes 0.2.1. Anything that is
// not purely numeric -- an untagged repo describing as a bare sha, or the "dev"
// fallback -- has no meaningful ordering, so it becomes 0.0.0.
func pkgVersion() string {
	v := strings.TrimPrefix(version(), "v")
	if i := strings.IndexByte(v, '-'); i >= 0 {
		v = v[:i]
	}
	if !numericVersion.MatchString(v) {
		return "0.0.0"
	}
	return v
}

// Build produces an unsigned macOS installer that puts the binary in
// /usr/local/bin and a launchd agent in /Library/LaunchAgents.
func (Pkg) Build() error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("pkg:build makes a macOS installer and needs Apple's tooling; this is %s", runtime.GOOS)
	}
	if err := sh.Rm(buildDir); err != nil {
		return err
	}

	// One universal binary, so the same pkg installs on Intel and Apple silicon.
	amd64Bin := filepath.Join(buildDir, "hue-scene-keeper-amd64")
	arm64Bin := filepath.Join(buildDir, "hue-scene-keeper-arm64")
	mg.Deps(
		mg.F(buildTo, "darwin", "amd64", amd64Bin),
		mg.F(buildTo, "darwin", "arm64", arm64Bin),
	)

	// The payload holds only what lives under /Library, whose directories are
	// already root:wheel. Anything under /usr/local is installed by the
	// postinstall script instead: a payload records an owner for every parent
	// directory it contains, so shipping /usr/local/bin would re-own it on
	// machines where it belongs to the user rather than to root.
	root := filepath.Join(buildDir, "pkgroot")
	if err := copyFile(
		filepath.Join("deploy", bundleID+".plist"),
		filepath.Join(root, "Library", "LaunchAgents", bundleID+".plist"),
		0o644,
	); err != nil {
		return err
	}

	// One universal binary rides along with the scripts for postinstall to
	// place. The example config is a reference copy only - the daemon runs
	// without a config file, so the installer never writes one into $HOME.
	scripts := filepath.Join(buildDir, "scripts")
	if err := os.MkdirAll(scripts, 0o755); err != nil {
		return err
	}
	// RunV, not Run: without the Xcode command line tools this is where the
	// build stops, and a bare exit status says nothing about what to install.
	if err := sh.RunV("lipo", "-create", "-output", filepath.Join(scripts, binary), amd64Bin, arm64Bin); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(scripts, binary), 0o755); err != nil {
		return err
	}
	if err := copyFile(exampleConfig, filepath.Join(scripts, exampleConfig), 0o644); err != nil {
		return err
	}
	for _, name := range []string{"preinstall", "postinstall"} {
		if err := copyFile(filepath.Join("deploy", "scripts", name), filepath.Join(scripts, name), 0o755); err != nil {
			return err
		}
	}

	component := filepath.Join(buildDir, "component.pkg")
	if err := sh.RunV("pkgbuild",
		"--root", root,
		"--scripts", scripts,
		"--identifier", bundleID,
		"--version", pkgVersion(),
		"--ownership", "recommended",
		"--install-location", "/",
		component,
	); err != nil {
		return err
	}

	if err := os.MkdirAll(distDir, 0o755); err != nil {
		return err
	}
	if err := sh.RunV("productbuild",
		"--distribution", filepath.Join("deploy", "distribution.xml"),
		"--package-path", buildDir,
		"--resources", filepath.Join("deploy", "resources"),
		pkgPath(),
	); err != nil {
		return err
	}

	fmt.Printf("\nbuilt %s (unsigned)\n", pkgPath())
	return nil
}

// Install builds the installer and runs it against this machine.
func (Pkg) Install() error {
	mg.Deps(Pkg.Build)
	return sh.RunV("sudo", "installer", "-pkg", pkgPath(), "-target", "/")
}

// Uninstall removes everything the installer put on this machine. Config and
// credentials under $HOME are left alone: they are the user's, not the
// package's, and a reinstall should find them still there.
func (Pkg) Uninstall() error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("pkg:uninstall removes a macOS install; this is %s", runtime.GOOS)
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), bundleID)
	// Best effort: the agent may not be loaded, which is not a failure.
	_ = sh.Run("launchctl", "bootout", target)
	// `hue-scene-keeper stop` writes the label into launchd's per-user disabled
	// database, and that database outlives the plist: booting out and deleting
	// the file leaves the entry behind, so a later install by hand fails its
	// bootstrap with "Bootstrap failed: 5: Input/output error" and nothing on
	// disk explains why. Uninstalling has to take the label with it.
	_ = sh.Run("launchctl", "enable", target)

	return sh.RunV("sudo", "/bin/sh", "-c", strings.Join([]string{
		"rm -f /Library/LaunchAgents/" + bundleID + ".plist",
		"rm -f /usr/local/bin/" + binary,
		"rm -rf /usr/local/share/" + binary,
		"pkgutil --forget " + bundleID + " >/dev/null 2>&1 || true",
	}, " && "))
}

// copyFile copies src to dst, creating parents and forcing the mode even if
// dst already exists.
func copyFile(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.WriteFile(dst, b, mode); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}
