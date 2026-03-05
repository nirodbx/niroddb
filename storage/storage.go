// Package storage implements the SSTable (Sorted String Table) file format.
// When the memtable is full, it's flushed to an immutable SSTable on disk.
// Reads check memtable first, then check each SSTable's Bloom filter before
// doing any disk I/O — skipping SSTables that definitely don't have the key.
//
// SSTable file format (v2 with Bloom filter):
//   [Data Block]
//     repeated: [KeyLen 4B][ValLen 4B][Deleted 1B][Key][Value]
//   [Index Block]
//     repeated: [KeyLen 4B][Offset 8B][Key]
//   [Bloom Block]
//     [bloom bytes]
//   [Footer]
//     [IndexOffset 8B][IndexLen 8B][BloomOffset 8B][BloomLen 8B][Magic 8B]
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/niroddb/niroddb/bloom"
	"github.com/niroddb/niroddb/memtable"
)

const magic = uint64(0x4E49524F44420002) // v2: includes bloom filter

// Entry mirrors memtable.Entry for storage layer use.
type Entry = memtable.Entry

// indexEntry maps a key to its byte offset in the SSTable data block.
type indexEntry struct {
	Key    string
	Offset int64
}

// SSTable is an immutable sorted file on disk.
type SSTable struct {
	path   string
	index  []indexEntry
	filter *bloom.Filter // in-memory bloom filter for this SSTable
}

// openSSTable loads the index and bloom filter of an existing SSTable file.
func openSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read footer (last 40 bytes for v2)
	if _, err := f.Seek(-40, io.SeekEnd); err != nil {
		return nil, err
	}
	var footer [40]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return nil, err
	}

	idxOffset  := int64(binary.LittleEndian.Uint64(footer[0:8]))
	idxLen     := int64(binary.LittleEndian.Uint64(footer[8:16]))
	blmOffset  := int64(binary.LittleEndian.Uint64(footer[16:24]))
	blmLen     := int64(binary.LittleEndian.Uint64(footer[24:32]))
	mag        := binary.LittleEndian.Uint64(footer[32:40])

	if mag != magic {
		return nil, fmt.Errorf("storage: invalid magic in %s (old format?)", path)
	}

	// Read index block
	indexData := make([]byte, idxLen)
	if _, err := f.ReadAt(indexData, idxOffset); err != nil {
		return nil, err
	}

	var index []indexEntry
	for pos := 0; pos < len(indexData); {
		if pos+12 > len(indexData) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(indexData[pos : pos+4]))
		offset := int64(binary.LittleEndian.Uint64(indexData[pos+4 : pos+12]))
		pos += 12
		if pos+keyLen > len(indexData) {
			break
		}
		key := string(indexData[pos : pos+keyLen])
		pos += keyLen
		index = append(index, indexEntry{Key: key, Offset: offset})
	}

	// Read bloom filter block
	bloomData := make([]byte, blmLen)
	if _, err := f.ReadAt(bloomData, blmOffset); err != nil {
		return nil, err
	}
	filter := bloom.NewFromBytes(bloomData)

	return &SSTable{path: path, index: index, filter: filter}, nil
}

// Get searches for a key in this SSTable.
// Returns (nil, false, nil) immediately if bloom filter says key is absent.
func (s *SSTable) Get(key string) ([]byte, bool, error) {
	// Bloom filter check — O(1), no disk I/O
	if !s.filter.Has(key) {
		return nil, false, nil // definitely not here
	}

	if len(s.index) == 0 {
		return nil, false, nil
	}

	// Binary search index for nearest key
	idx := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key > key
	}) - 1
	if idx < 0 {
		idx = 0
	}

	f, err := os.Open(s.path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	if _, err := f.Seek(s.index[idx].Offset, io.SeekStart); err != nil {
		return nil, false, err
	}

	for {
		var header [9]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(header[0:4]))
		valLen := int(binary.LittleEndian.Uint32(header[4:8]))
		deleted := header[8] == 1

		kv := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(f, kv); err != nil {
			break
		}
		k := string(kv[:keyLen])
		v := kv[keyLen:]

		if k == key {
			if deleted {
				return nil, false, nil
			}
			return v, true, nil
		}
		if k > key {
			break
		}
	}
	return nil, false, nil
}

// StorageEngine is the public API.
type StorageEngine struct {
	mu       sync.RWMutex
	dir      string
	sstables []*SSTable
}

// Open initialises or reopens a NiroDB data directory.
func Open(dir string) (*StorageEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	eng := &StorageEngine{dir: dir}
	if err := eng.loadSSTables(); err != nil {
		return nil, err
	}
	return eng, nil
}

// loadSSTables scans the data directory and loads all .sst files.
func (e *StorageEngine) loadSSTables() error {
	matches, err := filepath.Glob(filepath.Join(e.dir, "*.sst"))
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	for _, path := range matches {
		sst, err := openSSTable(path)
		if err != nil {
			fmt.Printf("storage: skipping SSTable %s: %v\n", path, err)
			continue
		}
		e.sstables = append(e.sstables, sst)
	}
	return nil
}

// Get retrieves a value: checks SSTables newest → oldest, using bloom filters.
func (e *StorageEngine) Get(key string) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, sst := range e.sstables {
		val, found, err := sst.Get(key)
		if err != nil {
			continue
		}
		if found {
			return val, true
		}
	}
	return nil, false
}

// Keys returns all live keys across SSTables (deduplicated).
func (e *StorageEngine) Keys() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	seen := make(map[string]struct{})
	// iterate oldest→newest so newer values win
	for i := len(e.sstables) - 1; i >= 0; i-- {
		entries, err := readAllEntries(e.sstables[i].path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.Deleted {
				delete(seen, entry.Key)
			} else {
				seen[entry.Key] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

// FlushEntries writes entries to a new SSTable on disk (with bloom filter).
func (e *StorageEngine) FlushEntries(entries []Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	ts := time.Now().UnixNano()
	path := filepath.Join(e.dir, fmt.Sprintf("%020d.sst", ts))

	if err := writeSSTable(path, entries); err != nil {
		return fmt.Errorf("storage: flush: %w", err)
	}

	sst, err := openSSTable(path)
	if err != nil {
		return err
	}

	e.sstables = append([]*SSTable{sst}, e.sstables...)
	return nil
}

// ShouldFlush always returns false — flush decisions are made by niroddb.go.
func (e *StorageEngine) ShouldFlush() bool { return false }

// writeSSTable serialises entries to an SSTable file with a bloom filter.
func writeSSTable(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Build bloom filter for all keys
	filter := bloom.New(len(entries))
	for _, e := range entries {
		filter.Add(e.Key)
	}

	var index []indexEntry
	var offset int64

	// Data block
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

	// Index block
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

	// Bloom filter block
	blmOffset := offset
	blmBytes := filter.Bytes()
	n, _ := f.Write(blmBytes)
	offset += int64(n)
	blmLen := offset - blmOffset

	// Footer (40 bytes)
	footer := make([]byte, 40)
	binary.LittleEndian.PutUint64(footer[0:8],  uint64(idxOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(idxLen))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(blmOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(blmLen))
	binary.LittleEndian.PutUint64(footer[32:40], magic)
	_, err = f.Write(footer)
	return err
}

// readAllEntries reads every entry from an SSTable (used by Keys() and compaction).
func readAllEntries(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read footer to find index offset (= end of data block)
	if _, err := f.Seek(-40, io.SeekEnd); err != nil {
		return nil, err
	}
	var footer [40]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return nil, err
	}
	idxOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))

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
