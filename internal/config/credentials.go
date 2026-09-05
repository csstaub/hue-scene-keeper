package config

import (
	"encoding/json"
	"errors"
	"fmt"
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
func LoadCredentials(path string) (*Credentials, error) {
	creds := &Credentials{path: path}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return creds, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, creds); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	creds.path = path
	return creds, nil
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
		// Refuse to write a blank key over a working one.
		if existing, err := LoadCredentials(c.path); err == nil && existing.AppKey != "" {
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
