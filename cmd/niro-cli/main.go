// cmd/niro-cli/main.go — NiroDB TCP client
// Usage: go run ./cmd/niro-cli --addr localhost:6380
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "localhost:6380", "NiroDB server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to NiroDB at %s\n", *addr)
	fmt.Println("Type HELP for available commands, EXIT to quit.")

	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("niroddb(%s)> ", *addr)
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.ToUpper(line) == "HELP" {
			printHelp()
			continue
		}

		// Send command to server
		fmt.Fprintf(conn, "%s\r\n", line)

		// Read response lines until we have a complete response
		response := readResponse(reader)
		fmt.Println(response)

		if strings.ToUpper(strings.TrimSpace(line)) == "EXIT" ||
			strings.ToUpper(strings.TrimSpace(line)) == "QUIT" {
			break
		}
	}
}

// readResponse reads a complete RESP-lite response from the server.
func readResponse(r *bufio.Reader) string {
	line, err := r.ReadString('\n')
	if err != nil {
		return "(error reading response)"
	}
	line = strings.TrimRight(line, "\r\n")

	switch {
	case strings.HasPrefix(line, "+"):
		// Simple string
		return line[1:]

	case strings.HasPrefix(line, "-"):
		// Error
		return "(error) " + line[1:]

	case strings.HasPrefix(line, "$"):
		// Bulk string
		if line == "$-1" {
			return "(nil)"
		}
		data, _ := r.ReadString('\n')
		return `"` + strings.TrimRight(data, "\r\n") + `"`

	case strings.HasPrefix(line, "*"):
		// Array
		count := 0
		fmt.Sscanf(line[1:], "%d", &count)
		if count == 0 {
			return "(empty)"
		}
		var sb strings.Builder
		for i := 0; i < count; i++ {
			// Read $len line
			lenLine, _ := r.ReadString('\n')
			lenLine = strings.TrimRight(lenLine, "\r\n")
			if lenLine == "$-1" {
				sb.WriteString(fmt.Sprintf("%d) (nil)\n", i+1))
				continue
			}
			// Read value line
			val, _ := r.ReadString('\n')
			val = strings.TrimRight(val, "\r\n")
			sb.WriteString(fmt.Sprintf("%d) %q\n", i+1, val))
		}
		return strings.TrimRight(sb.String(), "\n")

	case strings.HasPrefix(line, ":"):
		// Integer
		return "(integer) " + line[1:]

	default:
		// fallback: keep reading until blank line (multi-line stats)
		var sb strings.Builder
		sb.WriteString(line + "\n")
		for {
			next, err := r.ReadString('\n')
			if err != nil || strings.TrimSpace(next) == "" {
				break
			}
			if strings.HasPrefix(next, "+") {
				sb.WriteString(strings.TrimRight(next[1:], "\r\n") + "\n")
			} else {
				sb.WriteString(strings.TrimRight(next, "\r\n") + "\n")
			}
		}
		return strings.TrimRight(sb.String(), "\n")
	}
}

func printHelp() {
	fmt.Println(`Commands:
  SET <key> <value>   — store a value
  GET <key>           — retrieve a value
  DEL <key>           — delete a key
  KEYS                — list all keys
  FLUSH               — flush memtable to disk
  STATS               — show engine stats
  PING [msg]          — health check
  EXIT                — disconnect`)
}
