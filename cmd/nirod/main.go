// cmd/nirod/main.go — NiroDB server daemon
// Usage: go run ./cmd/nirod --dir /tmp/mydb --addr :6380
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	niroddb "github.com/niroddb/niroddb"
	"github.com/niroddb/niroddb/server"
)

func main() {
	dir := flag.String("dir", "./niroddata", "NiroDB data directory")
	addr := flag.String("addr", ":6380", "TCP address to listen on")
	flag.Parse()

	// Open database
	db, err := niroddb.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Start TCP server in background
	srv := server.New(db, *addr)

	go func() {
		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Printf("╔══════════════════════════════════╗\n")
	fmt.Printf("║       NiroDB Server v0.1.0       ║\n")
	fmt.Printf("║  addr: %-25s ║\n", *addr)
	fmt.Printf("║  dir:  %-25s ║\n", *dir)
	fmt.Printf("║  Press Ctrl+C to stop            ║\n")
	fmt.Printf("╚══════════════════════════════════╝\n")

	// Wait for Ctrl+C or SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[NiroDB] Shutting down...")
	srv.Stop()
	db.Close()
	fmt.Println("[NiroDB] Goodbye!")
}
