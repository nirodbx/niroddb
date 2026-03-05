// Package niroddb is the top-level NiroDB engine.
// It wires the memtable, WAL, storage, and compaction into a single DB interface.
//
// Write path:
//   1. Append to WAL (durable)
//   2. Write to memtable (fast in-memory)
//   3. If memtable full → flush to SSTable on disk
//   4. If SSTable count >= 4 → trigger background compaction
//
// Read path:
//   1. Check memtable
//   2. Check SSTables (newest → oldest)
//
// Recovery path (on Open):
//   1. Load SSTables from disk
//   2. Replay WAL into fresh memtable
package niroddb

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/niroddb/niroddb/compaction"
	"github.com/niroddb/niroddb/memtable"
	"github.com/niroddb/niroddb/storage"
	"github.com/niroddb/niroddb/wal"
)

// DB is the main NiroDB database handle.
type DB struct {
	mu         sync.Mutex
	mem        *memtable.SkipList
	wal        *wal.WAL
	storage    *storage.StorageEngine
	dir        string
	closed     bool
	compacting bool
}

// Open opens or creates a NiroDB database in the given directory.
func Open(dir string) (*DB, error) {
	eng, err := storage.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("niroddb: open storage: %w", err)
	}

	walPath := filepath.Join(dir, "niro.wal")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("niroddb: open wal: %w", err)
	}

	mem := memtable.New()
	if err := w.Replay(func(rec wal.Record) {
		switch rec.Op {
		case wal.OpSet:
			mem.Set(rec.Key, rec.Value)
		case wal.OpDelete:
			mem.Delete(rec.Key)
		}
	}); err != nil {
		return nil, fmt.Errorf("niroddb: wal replay: %w", err)
	}

	db := &DB{mem: mem, wal: w, storage: eng, dir: dir}
	fmt.Printf("[NiroDB] Opened at %q | WAL replayed | SSTables loaded\n", dir)
	return db, nil
}

// Set stores a key-value pair durably.
func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("niroddb: database is closed")
	}
	if err := db.wal.Append(wal.Record{Op: wal.OpSet, Key: key, Value: value}); err != nil {
		return fmt.Errorf("niroddb: wal write: %w", err)
	}
	db.mem.Set(key, value)
	if db.mem.ShouldFlush() {
		if err := db.flush(); err != nil {
			return err
		}
		go db.maybeCompact()
	}
	return nil
}

// Get retrieves a value by key.
func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if val, ok := db.mem.Get(key); ok {
		return val, true
	}
	return db.storage.Get(key)
}

// Delete removes a key.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("niroddb: database is closed")
	}
	if err := db.wal.Append(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("niroddb: wal write: %w", err)
	}
	db.mem.Delete(key)
	return nil
}

// Keys returns all live keys (memtable + SSTables, deduplicated).
func (db *DB) Keys() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	seen := make(map[string]struct{})
	for _, k := range db.mem.Keys() {
		seen[k] = struct{}{}
	}
	for _, k := range db.storage.Keys() {
		seen[k] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	return result
}

// Flush explicitly flushes the memtable to an SSTable on disk.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flush()
}

// Compact manually triggers compaction of all SSTables.
func (db *DB) Compact() (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.compacting = true
	defer func() { db.compacting = false }()
	stats, err := compaction.Run(db.dir)
	if err != nil {
		return "", err
	}
	// Reload SSTables after compaction
	db.storage, _ = storage.Open(db.dir)
	return stats.String(), nil
}

func (db *DB) flush() error {
	if db.mem.Len() == 0 {
		return nil
	}
	if err := db.storage.FlushEntries(db.mem.All()); err != nil {
		return fmt.Errorf("niroddb: flush: %w", err)
	}
	if err := db.wal.Reset(); err != nil {
		return fmt.Errorf("niroddb: wal reset: %w", err)
	}
	db.mem = memtable.New()
	fmt.Printf("[NiroDB] Flushed memtable → SSTable\n")
	return nil
}

func (db *DB) maybeCompact() {
	db.mu.Lock()
	if db.compacting || db.closed {
		db.mu.Unlock()
		return
	}
	needed, err := compaction.NeedsCompaction(db.dir)
	if err != nil || !needed {
		db.mu.Unlock()
		return
	}
	db.compacting = true
	db.mu.Unlock()

	defer func() {
		db.mu.Lock()
		db.compacting = false
		db.mu.Unlock()
	}()

	stats, err := compaction.Run(db.dir)
	if err != nil {
		fmt.Printf("[NiroDB] compaction error: %v\n", err)
		return
	}
	fmt.Printf("[NiroDB] %s\n", stats)

	db.mu.Lock()
	db.storage, _ = storage.Open(db.dir)
	db.mu.Unlock()
}

// Stats returns runtime statistics.
func (db *DB) Stats() map[string]interface{} {
	db.mu.Lock()
	defer db.mu.Unlock()
	needed, _ := compaction.NeedsCompaction(db.dir)
	return map[string]interface{}{
		"mem_keys":          db.mem.Len(),
		"mem_size_kb":       db.mem.Size() / 1024,
		"dir":               db.dir,
		"compaction_needed": needed,
		"compacting":        db.compacting,
	}
}

// Close flushes pending data and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	if err := db.flush(); err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		return err
	}
	db.closed = true
	fmt.Printf("[NiroDB] Closed cleanly.\n")
	return nil
}
