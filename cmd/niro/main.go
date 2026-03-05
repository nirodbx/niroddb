// cmd/niro/main.go — NiroDB interactive demo CLI
// Usage: go run ./cmd/niro --dir /tmp/nirodemo
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	niroddb "github.com/niroddb/niroddb"
)

func main() {
	dir := flag.String("dir", "./niroddata", "NiroDB data directory")
	flag.Parse()

	db, err := niroddb.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening NiroDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("╔══════════════════════════════╗")
	fmt.Println("║       NiroDB v0.1.0          ║")
	fmt.Println("║  Type HELP for commands      ║")
	fmt.Println("╚══════════════════════════════╝")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("niroddb> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) < 3 {
				fmt.Println("Usage: SET <key> <value>")
				continue
			}
			if err := db.Set(parts[1], []byte(parts[2])); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			val, ok := db.Get(parts[1])
			if !ok {
				fmt.Println("(nil)")
			} else {
				fmt.Printf("%q\n", string(val))
			}

		case "DEL", "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DEL <key>")
				continue
			}
			if err := db.Delete(parts[1]); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "KEYS":
			keys := db.Keys()
			if len(keys) == 0 {
				fmt.Println("(empty)")
			} else {
				for i, k := range keys {
					fmt.Printf("%d) %q\n", i+1, k)
				}
			}

		case "FLUSH":
			if err := db.Flush(); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK flushed to disk")
			}

		case "STATS":
			stats := db.Stats()
			for k, v := range stats {
				fmt.Printf("  %-20s %v\n", k, v)
			}

		case "PING":
			fmt.Println("PONG")

		case "HELP":
			fmt.Println(`Commands:
  SET <key> <value>   — store a value
  GET <key>           — retrieve a value
  DEL <key>           — delete a key
  KEYS                — list all keys
  FLUSH               — flush memtable to disk
  STATS               — show engine stats
  PING                — health check
  EXIT / QUIT         — close and exit`)

		case "EXIT", "QUIT":
			fmt.Println("Bye!")
			return

		default:
			fmt.Printf("Unknown command: %q (type HELP)\n", cmd)
		}
	}
}
