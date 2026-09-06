// Package logfile writes a log file that stays under a size cap.
//
// It is not a rotator. There is the live file and exactly one archive beside
// it, so the bytes on disk are bounded at twice the cap and nothing older
// survives. The daemon runs for months on a machine nobody administers. The
// failure worth preventing there is a log that grows until the disk is full,
// not the loss of last quarter's history.
package logfile

import (
	"os"
	"path/filepath"
	"sync"
)

// DefaultMaxBytes is the cap applied when Open is given a non-positive one.
const DefaultMaxBytes int64 = 8 << 20

// archiveSuffix is appended to the log path to name the one file kept behind.
const archiveSuffix = ".1"

// filePerm matches what a shell redirect under the usual umask would create,
// since that is what this replaced.
const filePerm os.FileMode = 0o644

// File is an io.Writer that keeps its file at or near a size cap.
//
// Each Write is one indivisible record. The size check runs before the write
// rather than after, so a record is never split across a rotation. That relies
// on slog's handlers calling Write once per complete log line, which they do.
type File struct {
	path string
	max  int64

	mu  sync.Mutex
	cur *os.File
	n   int64
}

// Open opens path for appending, creating it and its directory if needed. A
// max of zero or less means DefaultMaxBytes.
func Open(path string, max int64) (*File, error) {
	if max <= 0 {
		max = DefaultMaxBytes
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return nil, err
	}
	// Seed the count from what is already there. Starting at zero lets a
	// daemon that restarts often append a fresh cap's worth each time to a
	// file it believes is empty, and the cap never applies at all. launchd
	// restarts an unpaired daemon every 30 seconds, so that is not academic.
	var n int64
	if st, err := f.Stat(); err == nil {
		n = st.Size()
	}
	return &File{path: path, max: max, cur: f, n: n}, nil
}

// Write appends p, rotating first if it would carry the file past the cap.
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// The n > 0 guard stops a record larger than the whole cap from renaming
	// an empty live file over a good archive on every single write. Such a
	// record is written whole and overshoots instead. The alternative is
	// dropping it, and an oversized record (a stack, a bridge error body) is
	// usually the one worth having.
	if f.n > 0 && f.n+int64(len(p)) > f.max {
		f.rotate()
	}
	n, err := f.cur.Write(p)
	f.n += int64(n)
	return n, err
}

// Close closes the current file.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur.Close()
}

// rotate moves the live file aside and starts a fresh one. It is called with
// the lock held and never reports failure, because there is nobody to report
// it to. slog discards the error a handler's Write returns. So a rotation that
// could not happen degrades into carrying on, rather than into an error
// dropped silently along with the line that provoked it.
//
// The rename comes first and the old handle is kept until the new file is
// open. Closing first, or renaming and hoping the open succeeds, both leave a
// window where a failure strands the daemon with no file to log to. This
// ordering has none.
func (f *File) rotate() {
	old := f.cur
	// Rename replaces any existing archive in one step, so there is never a
	// moment with two archives or none.
	if err := os.Rename(f.path, f.path+archiveSuffix); err != nil {
		// ENOSPC, or the directory taken out from under us. Keep the handle
		// we have and defer the next attempt by a whole cap's worth of bytes,
		// rather than retrying a failing syscall on every line for as long as
		// the condition lasts.
		f.n = 0
		return
	}
	next, err := os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		// The archive is now the only file there is. Put it back under the
		// live name and carry on writing to it. old still refers to that same
		// inode, having followed both renames.
		_ = os.Rename(f.path+archiveSuffix, f.path)
		f.n = 0
		return
	}
	f.cur, f.n = next, 0
	_ = old.Close()
}
