package memtable_test

import (
	"fmt"
	"testing"

	"github.com/niroddb/niroddb/memtable"
)

func TestSetAndGet(t *testing.T) {
	sl := memtable.New()
	sl.Set("hello", []byte("world"))

	val, ok := sl.Get("hello")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestOverwrite(t *testing.T) {
	sl := memtable.New()
	sl.Set("k", []byte("v1"))
	sl.Set("k", []byte("v2"))

	val, _ := sl.Get("k")
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %q", val)
	}
}

func TestDelete(t *testing.T) {
	sl := memtable.New()
	sl.Set("x", []byte("data"))
	sl.Delete("x")

	_, ok := sl.Get("x")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestKeys_Sorted(t *testing.T) {
	sl := memtable.New()
	keys := []string{"banana", "apple", "cherry", "avocado"}
	for _, k := range keys {
		sl.Set(k, []byte(k))
	}

	got := sl.Keys()
	want := []string{"apple", "avocado", "banana", "cherry"}
	for i, k := range want {
		if got[i] != k {
			t.Fatalf("keys[%d] = %q, want %q", i, got[i], k)
		}
	}
}

func TestLargeInsert(t *testing.T) {
	sl := memtable.New()
	n := 10_000
	for i := 0; i < n; i++ {
		sl.Set(fmt.Sprintf("key:%06d", i), []byte(fmt.Sprintf("val:%d", i)))
	}
	if sl.Len() != n {
		t.Fatalf("expected %d keys, got %d", n, sl.Len())
	}

	// Spot check
	val, ok := sl.Get("key:005000")
	if !ok || string(val) != "val:5000" {
		t.Fatalf("unexpected value: %q %v", val, ok)
	}
}
