// Package memtable implements an in-memory sorted key-value store
// using a skip list for O(log n) reads and writes.
// Data in the memtable is flushed to disk when it exceeds MaxSize.
package memtable

import (
	"math/rand"
	"sync"
)

const (
	MaxLevel   = 16
	Probability = 0.5
	MaxSize    = 4 * 1024 * 1024 // 4MB default flush threshold
)

// Entry represents a single key-value record.
// Deleted entries are kept as tombstones until compaction.
type Entry struct {
	Key     string
	Value   []byte
	Deleted bool
}

// node is an internal skip list node.
type node struct {
	entry   Entry
	forward []*node
}

// SkipList is a probabilistic sorted data structure.
// Average O(log n) for insert, delete, and search.
type SkipList struct {
	head    *node
	level   int
	length  int
	maxSize int
	size    int // approximate byte size of stored data
	mu      sync.RWMutex
	rng     *rand.Rand
}

// New creates a new SkipList memtable.
func New() *SkipList {
	head := &node{forward: make([]*node, MaxLevel)}
	return &SkipList{
		head:    head,
		level:   1,
		maxSize: MaxSize,
		rng:     rand.New(rand.NewSource(42)),
	}
}

// randomLevel generates a random level for a new node.
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < MaxLevel && sl.rng.Float64() < Probability {
		level++
	}
	return level
}

// Set inserts or updates a key-value pair.
func (sl *SkipList) Set(key string, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*node, MaxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]

	// Update existing key
	if curr != nil && curr.entry.Key == key {
		sl.size -= len(curr.entry.Value)
		curr.entry.Value = value
		curr.entry.Deleted = false
		sl.size += len(value)
		return
	}

	// Insert new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &node{
		entry:   Entry{Key: key, Value: value},
		forward: make([]*node, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.length++
	sl.size += len(key) + len(value)
}

// Get retrieves a value by key.
// Returns (value, true) if found and not deleted, (nil, false) otherwise.
func (sl *SkipList) Get(key string) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < key {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr != nil && curr.entry.Key == key && !curr.entry.Deleted {
		return curr.entry.Value, true
	}
	return nil, false
}

// Delete marks a key as deleted (tombstone).
func (sl *SkipList) Delete(key string) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < key {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr != nil && curr.entry.Key == key && !curr.entry.Deleted {
		curr.entry.Deleted = true
		curr.entry.Value = nil
		sl.length--
		return true
	}
	return false
}

// Keys returns all non-deleted keys in sorted order.
func (sl *SkipList) Keys() []string {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	keys := make([]string, 0, sl.length)
	curr := sl.head.forward[0]
	for curr != nil {
		if !curr.entry.Deleted {
			keys = append(keys, curr.entry.Key)
		}
		curr = curr.forward[0]
	}
	return keys
}

// All returns all entries (including tombstones) for flushing to disk.
func (sl *SkipList) All() []Entry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	entries := make([]Entry, 0, sl.length)
	curr := sl.head.forward[0]
	for curr != nil {
		entries = append(entries, curr.entry)
		curr = curr.forward[0]
	}
	return entries
}

// Size returns approximate memory usage in bytes.
func (sl *SkipList) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// Len returns the number of live (non-deleted) entries.
func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// ShouldFlush returns true when memtable exceeds size threshold.
func (sl *SkipList) ShouldFlush() bool {
	return sl.Size() >= sl.maxSize
}
