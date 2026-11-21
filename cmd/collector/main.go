// Command collector runs the local suspended-solids collection service.
// Synchronization is triggered over HTTP and published batches are exposed
// for downstream consumers to read from their saved sequence position.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"estuary-collector/internal/service"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listen address")
	mock := flag.String("mock", "127.0.0.1:18081", "mock server address")
	dataDir := flag.String("data", "./data", "local data directory (state + JSONL batches)")
	rotate := flag.Int64("rotate-bytes", 0, "batch file rotation threshold in bytes (0 = 1 MiB default)")
	flag.Parse()

	cfg := service.DefaultConfig(*dataDir, *mock)
	cfg.RotateBytes = *rotate
	svc, err := service.New(cfg)
	if err != nil {
		log.Fatalf("init service: %v", err)
	}

	go func() {
		log.Printf("collector listening on http://%s, data dir %s", *addr, *dataDir)
		if err := svc.Start(*addr); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Print("stopped")
}
