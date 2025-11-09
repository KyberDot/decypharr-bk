package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/sirrobot01/decypharr/cmd/decypharr"
	"github.com/sirrobot01/decypharr/internal/config"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("FATAL: Recovered from panic in main: %v\n", r)
			debug.PrintStack()
		}
	}()

	var configPath string
	var pprofAddr string
	flag.StringVar(&configPath, "config", "/data", "path to the data folder")
	flag.StringVar(&pprofAddr, "pprof", ":6060", "pprof server address (set to empty to disable)")
	flag.Parse()

	config.SetConfigPath(configPath)
	config.Get()

	// Start pprof server if enabled
	if pprofAddr != "" {
		go func() {
			log.Printf("Starting pprof server on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	// Create a context canceled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := decypharr.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
