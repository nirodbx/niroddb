// Package compaction implements SSTable merging for NiroDB.
// After compaction, the new SSTable uses the v2 format with Bloom filter.
package compaction

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/niroddb/niroddb/bloom"
	"github.com/niroddb/niroddb/memtable"
)

const (
	TriggerCount = 4
	magic        = uint64(0x4E49524F44420002) // v2
)

type Entry = memtable.Entry

// Stats holds compaction result metrics.
type Stats struct {
	InputFiles  int
	InputKeys   int
	OutputKeys  int
	DroppedKeys int
	BytesBefore int64
	BytesAfter  int64
	Duration    time.Duration
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"compaction: %d files → 1 | keys: %d → %d (dropped %d) | size: %s → %s | took %s",
		s.InputFiles, s.InputKeys, s.OutputKeys, s.DroppedKeys,
		humanBytes(s.BytesBefore), humanBytes(s.BytesAfter),
		s.Duration.Round(time.Millisecond),
	)
}

// Run performs a full compaction of all SSTable files in dir.
func Run(dir string) (Stats, error) {
	start := time.Now()

	files, err := sstFiles(dir)
	if err != nil {
		return Stats{}, err
	}
	if len(files) < 2 {
		return Stats{}, fmt.Errorf("compaction: need at least 2 SSTables, found %d", len(files))
	}

	stats := Stats{InputFiles: len(files)}
	for _, f := range files {
		if info, err := os.Stat(f); err == nil {
			stats.BytesBefore += info.Size()
		}
	}

	// Read and merge all entries (oldest first, newest wins)
	merged := make(map[string]Entry)
	for _, path := range files {
		entries, err := readSSTable(path)
		if err != nil {
			return stats, fmt.Errorf("compaction: read %s: %w", path, err)
		}
		stats.InputKeys += len(entries)
		for _, e := range entries {
			merged[e.Key] = e
		}
	}

	// Collect live entries, drop tombstones
	live := make([]Entry, 0, len(merged))
	for _, e := range merged {
		if !e.Deleted {
			live = append(live, e)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Key < live[j].Key })

	stats.OutputKeys = len(live)
	stats.DroppedKeys = stats.InputKeys - stats.OutputKeys

	// Write merged SSTable
	ts := time.Now().UnixNano()
	outPath := filepath.Join(dir, fmt.Sprintf("%020d.sst", ts))
	tmpPath := outPath + ".tmp"

	if err := writeSSTable(tmpPath, live); err != nil {
		os.Remove(tmpPath)
		return stats, fmt.Errorf("compaction: write: %w", err)
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		os.Remove(tmpPath)
		return stats, fmt.Errorf("compaction: rename: %w", err)
	}

	if info, err := os.Stat(outPath); err == nil {
		stats.BytesAfter = info.Size()
	}

	// Remove old files
	for _, path := range files {
		os.Remove(path)
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

// NeedsCompaction returns true if dir has >= TriggerCount SSTable files.
func NeedsCompaction(dir string) (bool, error) {
	files, err := sstFiles(dir)
	if err != nil {
		return false, err
	}
	return len(files) >= TriggerCount, nil
}

func sstFiles(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	return matches, nil
}

func readSSTable(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Try v2 footer (40 bytes) first
	if _, err := f.Seek(-40, io.SeekEnd); err != nil {
		return nil, err
	}
	var footer [40]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return nil, err
	}

	var idxOffset int64
	mag := binary.LittleEndian.Uint64(footer[32:40])
	if mag == magic {
		// v2 format
		idxOffset = int64(binary.LittleEndian.Uint64(footer[0:8]))
	} else {
		// Try v1 footer (24 bytes)
		if _, err := f.Seek(-24, io.SeekEnd); err != nil {
			return nil, err
		}
		var footer1 [24]byte
		if _, err := io.ReadFull(f, footer1[:]); err != nil {
			return nil, err
		}
		idxOffset = int64(binary.LittleEndian.Uint64(footer1[0:8]))
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []Entry
	lr := io.LimitReader(f, idxOffset)
	for {
		var header [9]byte
		if _, err := io.ReadFull(lr, header[:]); err != nil {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(header[0:4]))
		valLen := int(binary.LittleEndian.Uint32(header[4:8]))
		deleted := header[8] == 1

		kv := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(lr, kv); err != nil {
			break
		}
		entries = append(entries, Entry{
			Key:     string(kv[:keyLen]),
			Value:   kv[keyLen:],
			Deleted: deleted,
		})
	}
	return entries, nil
}

func writeSSTable(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	filter := bloom.New(len(entries))
	for _, e := range entries {
		filter.Add(e.Key)
	}

	type indexEntry struct {
		Key    string
		Offset int64
	}

	var index []indexEntry
	var offset int64

	for _, entry := range entries {
		index = append(index, indexEntry{Key: entry.Key, Offset: offset})
		keyBytes := []byte(entry.Key)
		header := make([]byte, 9)
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(entry.Value)))
		if entry.Deleted {
			header[8] = 1
		}
		n1, _ := f.Write(header)
		n2, _ := f.Write(keyBytes)
		n3, _ := f.Write(entry.Value)
		offset += int64(n1 + n2 + n3)
	}

	idxOffset := offset
	for _, ie := range index {
		keyBytes := []byte(ie.Key)
		buf := make([]byte, 4+8+len(keyBytes))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint64(buf[4:12], uint64(ie.Offset))
		copy(buf[12:], keyBytes)
		n, _ := f.Write(buf)
		offset += int64(n)
	}
	idxLen := offset - idxOffset

	blmOffset := offset
	blmBytes := filter.Bytes()
	n, _ := f.Write(blmBytes)
	offset += int64(n)
	blmLen := offset - blmOffset

	footer := make([]byte, 40)
	binary.LittleEndian.PutUint64(footer[0:8],  uint64(idxOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(idxLen))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(blmOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(blmLen))
	binary.LittleEndian.PutUint64(footer[32:40], magic)
	_, err = f.Write(footer)
	return err
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
