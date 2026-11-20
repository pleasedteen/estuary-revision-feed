// Command mocksrv serves the three simulated institutions and the shared
// authentication endpoint. All data is local, deterministic and resettable.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"estuary-collector/internal/mockserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18081", "listen address")
	flag.Parse()

	srv := mockserver.New()
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("mock institutions + auth endpoint listening on http://%s", *addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
