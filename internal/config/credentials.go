package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// Credentials is the daemon's persisted trust in one bridge: the application
// key it was issued, and the TLS key it pinned on first contact.
type Credentials struct {
	BridgeID string `json:"bridge_id,omitempty"`
	Address  string `json:"address,omitempty"`
	AppKey   string `json:"app_key"`
	CertPin  string `json:"cert_pin,omitempty"`

	path string
}

// ErrNoCredentials reports that the daemon has not been paired yet.
var ErrNoCredentials = errors.New("no credentials found; run `hue-scene-keeper auth` first")

// LoadCredentials reads the credentials file. A missing file yields an empty,
// saveable Credentials rather than an error, so `auth` can populate it.
//
// A file other users can read is warned about, not rejected. Save is
// meticulous about 0600, but a file restored from a backup, copied between
// machines, or written by an older version arrives at whatever mode it arrives
// at, and the application key it holds is the whole of the daemon's authority
// over the bridge. Refusing to start would strand a working daemon over
// something one chmod fixes, so this follows ssh's lead and says so loudly
// instead.
func LoadCredentials(path string) (*Credentials, error) {
	creds := &Credentials{path: path}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return creds, nil
		}
		return nil, err
	}
	if info, err := os.Stat(path); err == nil && info.Mode().Perm()&0o077 != 0 {
		slog.Warn("credentials file is readable by other users; the bridge application key is a secret",
			"path", path, "mode", fmt.Sprintf("%04o", info.Mode().Perm()), "want", "0600")
	}
	if err := json.Unmarshal(raw, creds); err != nil {
		return nil, fmt.Errorf("parse %s: %w; delete the file and re-run `hue-scene-keeper auth` to pair again", path, err)
	}
	creds.path = path
	return creds, nil
}

// EnsureWritable checks the credentials path can actually be written, without
// writing anything to it.
//
// Pairing asks the user to press the link button and the bridge then issues an
// application key exactly once. If storing it fails at that point the key
// exists nowhere, the user has to press the button again, and the bridge is
// left carrying an orphan whitelist entry - so the check belongs before the
// prompt, not after the key is in hand.
func (c *Credentials) EnsureWritable() error {
	if c.path == "" {
		return errors.New("credentials have no path")
	}
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	return os.Remove(name)
}

// Save writes the credentials atomically at mode 0600.
//
// The temporary file is created with CreateTemp rather than a predictable
// "<path>.tmp": a fixed name is an open invitation to have the secret written
// through a pre-planted symlink, or into a file whose permissions someone else
// chose. Both file and directory are synced, because the whole point of this
// daemon is surviving power cuts, and a cut just after the rename could
// otherwise leave a zero-length credentials file and force re-pairing.
func (c *Credentials) Save() error {
	if c.path == "" {
		return errors.New("credentials have no path")
	}
	if c.AppKey == "" {
		// Refuse to write a blank key over a working one. A read error is not
		// permission to proceed: a file we cannot parse or open may well hold
		// the only copy of a working key, so the interlock has to fail closed.
		existing, err := LoadCredentials(c.path)
		switch {
		case err != nil && !os.IsNotExist(err):
			return fmt.Errorf("refusing to overwrite %s with an empty application key: cannot read it first: %w", c.path, err)
		case err == nil && existing.AppKey != "":
			return errors.New("refusing to overwrite existing credentials with an empty application key")
		}
	}
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')

	tmp, err := os.CreateTemp(dir, ".credentials-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, c.path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	// Sync the directory so the rename itself survives a power cut.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Path returns where the credentials live.
func (c *Credentials) Path() string { return c.path }

// SetPath points the credentials at a file, for a freshly built value.
func (c *Credentials) SetPath(p string) { c.path = p }
