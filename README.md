# NiroDB 🗄️

> A hand-built key-value storage engine written in Go — built from scratch to understand how databases really work under the hood.

NiroDB implements the core ideas behind LevelDB and RocksDB: an LSM-tree architecture with a memtable, Write-Ahead Log, SSTables, Bloom filters, and compaction — all in pure Go with zero external dependencies.

---

## Features

- **Skip List memtable** — O(log n) reads and writes, sorted keys
- **Write-Ahead Log (WAL)** — CRC32 per record, full crash recovery on restart
- **SSTable v2** — immutable sorted files on disk with per-file Bloom filters
- **Bloom Filter** — probabilistic index that eliminates unnecessary disk reads (~0.8% false positive rate)
- **Size-tiered Compaction** — merges SSTables, deduplicates keys, removes tombstones
- **TCP Server** — RESP-compatible protocol (same as Redis wire format)
- **Interactive CLI** — local shell for quick access
- **Thread-safe** — all operations protected with `sync.Mutex`

---

## Architecture

```
Write Path
──────────
Client → WAL (fsync) → Memtable (SkipList) ──[full?]──► SSTable (disk)
                                                              │
                                                    [4+ files?]
                                                              ▼
                                                        Compaction
                                                    (merge → 1 file)

Read Path
─────────
Client → Memtable ──[miss]──► Bloom[0]? ──[maybe]──► SSTable[0]
                              Bloom[1]? ──[maybe]──► SSTable[1]
                              Bloom[n]? → "definitely not" → skip ⚡

Recovery (on startup)
─────────────────────
Load SSTables from disk → Replay WAL → Restore Memtable state
```

---

## Getting Started

### Run as a TCP server

```bash
# Terminal 1 — start the server
go run ./cmd/nirod --dir /tmp/mydb --addr :6380

# Terminal 2 — connect with the CLI client
go run ./cmd/niro-cli --addr localhost:6380
```

```
niroddb(localhost:6380)> SET name NiroDB
OK
niroddb(localhost:6380)> GET name
"NiroDB"
niroddb(localhost:6380)> KEYS
1) "name"
niroddb(localhost:6380)> PING
PONG
niroddb(localhost:6380)> STATS
  dir                  /tmp/mydb
  mem_keys             1
  mem_size_kb          0
  compaction_needed    false
niroddb(localhost:6380)> COMPACT
compaction: 4 files → 1 | keys: 12 → 8 (dropped 4) | size: 1.2KB → 0.6KB | took 3ms
```

### Or use netcat directly (RESP protocol)

```bash
echo -e "SET foo bar\r" | nc localhost 6380
# +OK

echo -e "GET foo\r" | nc localhost 6380
# $3
# bar
```

### Run the local interactive CLI (no server needed)

```bash
go run ./cmd/niro --dir /tmp/mydb
```

---

## Commands

| Command | Description |
|---------|-------------|
| `SET key value` | Store a value |
| `GET key` | Retrieve a value |
| `DEL key` | Delete a key (tombstone) |
| `KEYS` | List all live keys |
| `FLUSH` | Force flush memtable → SSTable |
| `COMPACT` | Merge all SSTables into one |
| `STATS` | Show engine statistics |
| `PING [msg]` | Health check |
| `EXIT` | Disconnect |

---

## SSTable File Format (v2)

```
┌──────────────────────────────────────┐
│  Data Block                          │
│  [KeyLen 4B][ValLen 4B][Deleted 1B]  │
│  [Key bytes][Value bytes]            │
│  ... (one record per entry)          │
├──────────────────────────────────────┤
│  Index Block                         │
│  [KeyLen 4B][Offset 8B][Key bytes]   │
│  ... (one entry per key)             │
├──────────────────────────────────────┤
│  Bloom Filter Block                  │
│  [bit array][k as 1 byte]            │
├──────────────────────────────────────┤
│  Footer (40 bytes)                   │
│  [IndexOffset  8B]                   │
│  [IndexLen     8B]                   │
│  [BloomOffset  8B]                   │
│  [BloomLen     8B]                   │
│  [Magic 0x4E49524F44420002  8B]      │
└──────────────────────────────────────┘
```

---

## Project Structure

```
niroddb/
├── niroddb.go          # Main DB handle — wires all components
├── memtable/
│   ├── memtable.go     # Skip List (in-memory sorted KV store)
│   └── memtable_test.go
├── wal/
│   └── wal.go          # Write-Ahead Log (crash recovery)
├── storage/
│   └── storage.go      # SSTable engine (disk persistence + Bloom lookup)
├── bloom/
│   ├── bloom.go        # Bloom filter (~0.8% FP rate, 10 bits/key)
│   └── bloom_test.go
├── compaction/
│   └── compaction.go   # Size-tiered compaction
├── server/
│   └── server.go       # TCP server (RESP-lite protocol)
└── cmd/
    ├── niro/           # Local interactive CLI
    ├── niro-cli/       # TCP client CLI
    └── nirod/          # Server daemon
```

---

## Running Tests

```bash
go test ./...
```

---

## Roadmap

- [x] Skip List memtable
- [x] Write-Ahead Log + crash recovery
- [x] SSTable with index block
- [x] Bloom Filter per SSTable
- [x] Size-tiered compaction
- [x] TCP server (RESP protocol)
- [ ] TTL — auto-expiring keys
- [ ] SCAN — range queries with prefix
- [ ] Batch writes — atomic multi-key operations
- [ ] Benchmarks vs Redis / LevelDB

---

## Learning Resources

These are the references that inspired NiroDB's design:

- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [The Log-Structured Merge-Tree paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Chapter 3
- [Building a Database from Scratch](https://cstack.github.io/db_tutorial/)
