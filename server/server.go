// Package server implements a TCP server for NiroDB.
// Protocol is line-based (like Redis inline commands):
//   Client sends:  "SET key value\r\n"  or  "GET key\r\n"
//   Server replies: "+OK\r\n"  /  "$5\r\nvalue\r\n"  / "-ERR msg\r\n"
//
// Response types (RESP-lite):
//   +<msg>          simple string / OK
//   -<msg>          error
//   $<len>\r\n<data> bulk string (value)
//   *<n>\r\n        array of n bulk strings (for KEYS)
//   :0 / :1        integer (DEL result)
//   (nil)           null bulk string
package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	niroddb "github.com/niroddb/niroddb"
)

// Server wraps a NiroDB instance and listens for TCP connections.
type Server struct {
	db       *niroddb.DB
	listener net.Listener
	addr     string
	conns    int32 // active connections (atomic)
	quit     chan struct{}
}

// New creates a new Server bound to addr (e.g. ":6380").
func New(db *niroddb.DB, addr string) *Server {
	return &Server{
		db:   db,
		addr: addr,
		quit: make(chan struct{}),
	}
}

// Start begins listening for connections. Blocks until Stop() is called.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.addr, err)
	}
	s.listener = ln
	fmt.Printf("[NiroDB] TCP server listening on %s\n", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				fmt.Printf("[NiroDB] accept error: %v\n", err)
				continue
			}
		}
		atomic.AddInt32(&s.conns, 1)
		go s.handleConn(conn)
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
}

// ActiveConns returns the number of currently active connections.
func (s *Server) ActiveConns() int32 {
	return atomic.LoadInt32(&s.conns)
}

// handleConn processes all commands from a single client connection.
func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt32(&s.conns, -1)
	}()

	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("[NiroDB] client connected: %s\n", remoteAddr)

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Set a write deadline per command
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		response := s.dispatch(line)
		writer.WriteString(response)
		writer.Flush()
	}

	fmt.Printf("[NiroDB] client disconnected: %s\n", remoteAddr)
}

// dispatch parses a command line and routes to the appropriate handler.
func (s *Server) dispatch(line string) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) == 0 {
		return errResp("empty command")
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "SET":
		if len(parts) < 3 {
			return errResp("SET requires 2 args: SET <key> <value>")
		}
		if err := s.db.Set(parts[1], []byte(parts[2])); err != nil {
			return errResp(err.Error())
		}
		return okResp()

	case "GET":
		if len(parts) < 2 {
			return errResp("GET requires 1 arg: GET <key>")
		}
		val, ok := s.db.Get(parts[1])
		if !ok {
			return nilResp()
		}
		return bulkResp(val)

	case "DEL", "DELETE":
		if len(parts) < 2 {
			return errResp("DEL requires 1 arg: DEL <key>")
		}
		if err := s.db.Delete(parts[1]); err != nil {
			return errResp(err.Error())
		}
		return okResp()

	case "KEYS":
		keys := s.db.Keys()
		return arrayResp(keys)

	case "COMPACT":
		msg, err := s.db.Compact()
		if err != nil {
			return errResp(err.Error())
		}
		return simpleResp(msg)

	case "FLUSH":
		if err := s.db.Flush(); err != nil {
			return errResp(err.Error())
		}
		return okResp()

	case "PING":
		if len(parts) >= 2 {
			return simpleResp(parts[1]) // PING <msg> → <msg>
		}
		return simpleResp("PONG")

	case "STATS":
		stats := s.db.Stats()
		var sb strings.Builder
		for k, v := range stats {
			sb.WriteString(fmt.Sprintf("+%s=%v\r\n", k, v))
		}
		return sb.String()

	case "QUIT", "EXIT":
		return simpleResp("BYE")

	default:
		return errResp(fmt.Sprintf("unknown command %q", cmd))
	}
}

// ── Response helpers ──────────────────────────────────────────────────────────

func okResp() string           { return "+OK\r\n" }
func simpleResp(s string) string { return "+" + s + "\r\n" }
func errResp(msg string) string  { return "-ERR " + msg + "\r\n" }
func nilResp() string            { return "$-1\r\n" }

func bulkResp(data []byte) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func arrayResp(items []string) string {
	if len(items) == 0 {
		return "*0\r\n"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
	for _, item := range items {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}
	return sb.String()
}
