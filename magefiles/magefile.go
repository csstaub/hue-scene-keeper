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
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	binary  = "hue-scene-keeper"
	mainPkg = "./cmd/hue-scene-keeper"
	distDir = "dist"

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

// --- go: the Go toolchain --------------------------------------------------

// Build compiles the binary for this machine.
func (Go) Build() error {
	return sh.RunV("go", "build", "-ldflags", ldflags(), "-o", binary, mainPkg)
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

// Install builds the binary and installs it into /usr/local/bin.
func (Go) Install() error {
	mg.Deps(Go.Build)
	return sh.RunV("install", "-m0755", binary, filepath.Join("/usr/local/bin", binary))
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
	if err := sh.Run("lipo", "-create", "-output", filepath.Join(scripts, binary), amd64Bin, arm64Bin); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(scripts, binary), 0o755); err != nil {
		return err
	}
	if err := copyFile("config.example.yaml", filepath.Join(scripts, "config.example.yaml"), 0o644); err != nil {
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
	// Best effort: the agent may not be loaded, which is not a failure.
	_ = sh.Run("launchctl", "bootout", fmt.Sprintf("gui/%d/%s", os.Getuid(), bundleID))

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
