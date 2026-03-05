package bloom_test

import (
	"fmt"
	"testing"

	"github.com/niroddb/niroddb/bloom"
)

func TestBasic(t *testing.T) {
	f := bloom.New(100)
	keys := []string{"apple", "banana", "cherry", "durian", "elderberry"}
	for _, k := range keys {
		f.Add(k)
	}

	// All added keys must return true
	for _, k := range keys {
		if !f.Has(k) {
			t.Errorf("expected %q to be in filter", k)
		}
	}

	// Keys definitely not added should mostly return false
	misses := 0
	for i := 0; i < 1000; i++ {
		if !f.Has(fmt.Sprintf("notakey:%d", i)) {
			misses++
		}
	}
	// At least 99% should be correctly identified as absent
	if misses < 990 {
		t.Errorf("too many false positives: %d/1000 missed", 1000-misses)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	f := bloom.New(50)
	keys := []string{"foo", "bar", "baz"}
	for _, k := range keys {
		f.Add(k)
	}

	// Serialize and restore
	data := f.Bytes()
	f2 := bloom.NewFromBytes(data)

	for _, k := range keys {
		if !f2.Has(k) {
			t.Errorf("restored filter missing key %q", k)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	n := 1000
	f := bloom.New(n)
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key:%d", i))
	}

	fp := 0
	trials := 10000
	for i := n; i < n+trials; i++ {
		if f.Has(fmt.Sprintf("key:%d", i)) {
			fp++
		}
	}

	rate := float64(fp) / float64(trials)
	t.Logf("False positive rate: %.2f%% (%d/%d)", rate*100, fp, trials)

	// Should be well under 5%
	if rate > 0.05 {
		t.Errorf("false positive rate too high: %.2f%%", rate*100)
	}
}

func BenchmarkAdd(b *testing.B) {
	f := bloom.New(b.N)
	for i := 0; i < b.N; i++ {
		f.Add(fmt.Sprintf("key:%d", i))
	}
}

func BenchmarkHas(b *testing.B) {
	f := bloom.New(1000)
	for i := 0; i < 1000; i++ {
		f.Add(fmt.Sprintf("key:%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Has(fmt.Sprintf("key:%d", i%2000))
	}
}
