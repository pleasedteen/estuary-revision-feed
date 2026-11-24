// Command collector runs the local suspended-solids collection service.
// Synchronization is triggered over HTTP and published batches are exposed
// for downstream consumers to read from their saved sequence position.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"estuary-collector/internal/domain"
	"estuary-collector/internal/service"
	"estuary-collector/internal/state"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listen address")
	mock := flag.String("mock", "127.0.0.1:18081", "mock server address")
	dataDir := flag.String("data", "./data", "local data directory (state + JSONL batches)")
	flag.Parse()

	svc, err := service.New(service.DefaultConfig(*dataDir, *mock))
	if err != nil {
		log.Fatalf("init service: %v", err)
	}
	armChaosHook(svc)

	go func() {
		log.Printf("collector listening on http://%s, data dir %s", *addr, *dataDir)
		if err := svc.Start(*addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

// armChaosHook implements the drill knob ESTUARY_TEST_CRASH_AFTER:
//
//	ESTUARY_TEST_CRASH_AFTER=publish[:source]     exit right after a batch is
//	                                              durable and journaled, before
//	                                              the resume position is saved
//	ESTUARY_TEST_CRASH_AFTER=checkpoint[:source]  exit right after the resume
//	                                              position is saved and journaled
//
// The process exits immediately (like a kill -9) at the exact persistence
// boundary, so restart drills can verify that the run journal shows where
// the round stopped. Never set this outside failure-injection drills.
func armChaosHook(svc *service.Service) {
	spec := os.Getenv("ESTUARY_TEST_CRASH_AFTER")
	if spec == "" {
		return
	}
	stage, target, _ := strings.Cut(spec, ":")
	crash := func(source string) {
		if target == "" || target == source {
			log.Printf("chaos drill: exiting after %s boundary (source %s)", stage, source)
			os.Exit(2)
		}
	}
	switch stage {
	case "publish":
		svc.Engine.SetAfterPublishHook(func(source string, _ domain.Envelope) { crash(source) })
	case "checkpoint":
		svc.Engine.SetAfterCheckpointHook(func(source string, _ state.Position) { crash(source) })
	default:
		log.Printf("ignoring unknown ESTUARY_TEST_CRASH_AFTER=%q", spec)
	}
}
