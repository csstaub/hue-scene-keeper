package logfile

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write writes one record and fails the test if the whole of it did not land.
func write(t *testing.T, f *File, s string) {
	t.Helper()
	n, err := f.Write([]byte(s))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(s) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(s))
	}
}

// read returns the contents of path, or "" if it does not exist.
func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return string(b)
}

func TestRotatesBeforeCrossingTheCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	f, err := Open(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	write(t, f, "aaaa")
	write(t, f, "bbbb")
	// A third record would make 12 bytes against a cap of 10, so it must land
	// in a fresh file with the first two moved aside whole.
	write(t, f, "cccc")

	if got := read(t, path); got != "cccc" {
		t.Errorf("live file = %q, want %q", got, "cccc")
	}
	if got := read(t, path+".1"); got != "aaaabbbb" {
		t.Errorf("archive = %q, want %q", got, "aaaabbbb")
	}
}

func TestSizeIsSeededFromTheFileOnDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	if err := os.WriteFile(path, []byte("aaaaaaaa"), 0o644); err != nil {
		t.Fatal(err)
	}

	// A restart against an existing 8-byte file: one more record crosses a cap
	// of 10 and must rotate. Seeding from zero instead would append a whole
	// fresh cap's worth to a file already at the limit.
	f, err := Open(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	write(t, f, "bbbb")

	if got := read(t, path); got != "bbbb" {
		t.Errorf("live file = %q, want %q", got, "bbbb")
	}
	if got := read(t, path+".1"); got != "aaaaaaaa" {
		t.Errorf("archive = %q, want %q", got, "aaaaaaaa")
	}
}

func TestOversizedRecordIsWrittenWholeAndKeepsTheArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	f, err := Open(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	write(t, f, "aaaa")
	big := strings.Repeat("x", 100)
	write(t, f, big)

	// The record does not fit under any cap, so it overshoots rather than
	// being split or dropped, and the file it displaced is still there.
	if got := read(t, path); got != big {
		t.Errorf("live file = %d bytes, want the %d-byte record whole", len(got), len(big))
	}
	if got := read(t, path+".1"); got != "aaaa" {
		t.Errorf("archive = %q, want %q", got, "aaaa")
	}

	// Writing an oversized record into an empty file must not rename that
	// empty file over the archive.
	path2 := filepath.Join(t.TempDir(), "test.log")
	if err := os.WriteFile(path2+".1", []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	f2, err := Open(path2, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f2.Close() })
	write(t, f2, big)
	if got := read(t, path2+".1"); got != "keep me" {
		t.Errorf("archive = %q, want it untouched", got)
	}
}

func TestArchiveIsReplacedNotAccumulated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	f, err := Open(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	for _, s := range []string{"aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd"} {
		write(t, f, s)
	}

	if got := read(t, path); got != "dddddddd" {
		t.Errorf("live file = %q, want %q", got, "dddddddd")
	}
	if got := read(t, path+".1"); got != "cccccccc" {
		t.Errorf("archive = %q, want %q", got, "cccccccc")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory holds %v, want exactly the live file and one archive", names)
	}
}

func TestStaysUnderTwiceTheCap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	const max = 512
	f, err := Open(path, max)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	record := append(bytes.Repeat([]byte("x"), 63), '\n')
	for range 200 {
		write(t, f, string(record))
	}

	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	// Twice the cap, plus room for the one record that crosses the boundary.
	if limit := int64(2*max) + int64(len(record)); total > limit {
		t.Errorf("%d bytes on disk after 200 records, want at most %d", total, limit)
	}
}
