// Package bloom implements a space-efficient probabilistic Bloom filter.
//
// A Bloom filter answers "is this key in the set?" with two possible answers:
//   - "Definitely NOT in set" → 100% accurate, skip the SSTable entirely
//   - "Probably in set"       → may be a false positive, do the real disk read
//
// This eliminates unnecessary disk reads for keys that don't exist,
// which is the most common case in write-heavy workloads.
//
// How it works:
//   - A bit array of m bits, all initialized to 0
//   - k independent hash functions
//   - Add(key): set bits at positions h1(key), h2(key), ..., hk(key)
//   - Has(key): check if ALL bits at those positions are set
//     → if any bit is 0 → key is DEFINITELY not in set
//     → if all bits are 1 → key is PROBABLY in set
//
// False positive rate with m=10*n bits and k=7 hash functions ≈ 0.8%
package bloom

import (
	"math"
)

const (
	// bitsPerKey controls filter size. 10 bits/key → ~0.8% false positive rate.
	bitsPerKey = 10
	// numHashFunctions is the optimal k for bitsPerKey=10.
	numHashFunctions = 7
)

// Filter is a Bloom filter backed by a byte slice.
type Filter struct {
	bits []byte
	k    uint // number of hash functions
	m    uint // total number of bits
}

// New creates a Bloom filter sized for n expected keys.
func New(n int) *Filter {
	if n <= 0 {
		n = 1
	}
	m := uint(math.Ceil(float64(n) * bitsPerKey))
	// Round up to nearest byte
	m = ((m + 7) / 8) * 8

	return &Filter{
		bits: make([]byte, m/8),
		k:    numHashFunctions,
		m:    m,
	}
}

// NewFromBytes restores a Filter from a previously serialized byte slice.
func NewFromBytes(data []byte) *Filter {
	if len(data) < 1 {
		return New(1)
	}
	k := uint(data[len(data)-1])
	bits := make([]byte, len(data)-1)
	copy(bits, data[:len(data)-1])
	m := uint(len(bits)) * 8
	return &Filter{bits: bits, k: k, m: m}
}

// Add inserts a key into the filter.
func (f *Filter) Add(key string) {
	h := baseHash(key)
	delta := (h >> 17) | (h << 15) // rotate right 17
	for i := uint(0); i < f.k; i++ {
		pos := h % f.m
		f.bits[pos/8] |= 1 << (pos % 8)
		h += delta
	}
}

// Has returns false if the key is DEFINITELY not in the set.
// Returns true if the key is PROBABLY in the set (may be false positive).
func (f *Filter) Has(key string) bool {
	if f == nil || len(f.bits) == 0 {
		return true // no filter → assume present
	}
	h := baseHash(key)
	delta := (h >> 17) | (h << 15)
	for i := uint(0); i < f.k; i++ {
		pos := h % f.m
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false // definitely not present
		}
		h += delta
	}
	return true // probably present
}

// Bytes serializes the filter for storage alongside an SSTable.
// Format: [bit array][k as 1 byte]
func (f *Filter) Bytes() []byte {
	out := make([]byte, len(f.bits)+1)
	copy(out, f.bits)
	out[len(f.bits)] = byte(f.k)
	return out
}

// FalsePositiveRate returns the theoretical false positive rate
// given the number of inserted keys.
func (f *Filter) FalsePositiveRate(n int) float64 {
	if n == 0 {
		return 0
	}
	// (1 - e^(-k*n/m))^k
	exponent := -float64(f.k) * float64(n) / float64(f.m)
	return math.Pow(1-math.Exp(exponent), float64(f.k))
}

// baseHash is a fast non-cryptographic hash (murmur-inspired).
func baseHash(key string) uint {
	var h uint = 0xdeadbeef
	for i := 0; i < len(key); i++ {
		h ^= uint(key[i])
		h *= 0x5bd1e995
		h ^= h >> 15
	}
	return h
}
