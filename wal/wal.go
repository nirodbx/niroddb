// Package wal implements a Write-Ahead Log for crash recovery.
// Every write is appended to the WAL before being applied to the memtable.
// On restart, the WAL is replayed to reconstruct in-memory state.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// OpType identifies the type of WAL record.
type OpType byte

const (
	OpSet    OpType = 0x01
	OpDelete OpType = 0x02
)

// Record is a single WAL entry: [CRC32 | OpType | KeyLen | ValueLen | Key | Value]
// CRC32 covers everything after it (OpType + KeyLen + ValueLen + Key + Value).
type Record struct {
	Op    OpType
	Key   string
	Value []byte
}

// WAL is a sequential append-only log file.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
	path string
}

// Open opens or creates a WAL file at the given path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &WAL{
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
		path: path,
	}, nil
}

// Append writes a record to the WAL and syncs to disk.
func (w *WAL) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyBytes := []byte(rec.Key)
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(rec.Value))

	// Build payload for checksum
	payload := make([]byte, 1+4+4+keyLen+valLen)
	payload[0] = byte(rec.Op)
	binary.LittleEndian.PutUint32(payload[1:5], keyLen)
	binary.LittleEndian.PutUint32(payload[5:9], valLen)
	copy(payload[9:9+keyLen], keyBytes)
	copy(payload[9+keyLen:], rec.Value)

	checksum := crc32.ChecksumIEEE(payload)

	// Write: [CRC32 4B][payload]
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], checksum)

	if _, err := w.buf.Write(crcBuf[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}

	// Flush buffer and fsync
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads all records from the WAL and calls fn for each valid record.
// Corrupted or incomplete records at the tail (e.g. from a crash) are skipped.
func (w *WAL) Replay(fn func(Record)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r := bufio.NewReader(w.file)
	for {
		// Read CRC
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		storedCRC := binary.LittleEndian.Uint32(crcBuf[:])

		// Read op + keyLen + valLen
		header := make([]byte, 9)
		if _, err := io.ReadFull(r, header); err != nil {
			break
		}

		op := OpType(header[0])
		keyLen := binary.LittleEndian.Uint32(header[1:5])
		valLen := binary.LittleEndian.Uint32(header[5:9])

		data := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(r, data); err != nil {
			break
		}

		payload := append(header, data...)
		if crc32.ChecksumIEEE(payload) != storedCRC {
			fmt.Printf("wal: checksum mismatch, skipping corrupted record\n")
			continue
		}

		rec := Record{
			Op:    op,
			Key:   string(data[:keyLen]),
			Value: data[keyLen:],
		}
		fn(rec)
	}
	return nil
}

// Reset truncates the WAL after a successful memtable flush to disk.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	w.buf.Reset(w.file)
	return err
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Path returns the WAL file path.
func (w *WAL) Path() string {
	return w.path
}
