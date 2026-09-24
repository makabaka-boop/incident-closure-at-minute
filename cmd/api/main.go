// Command api runs the raincut HTTP service.
//
// The process drains in two phases. The first SIGTERM or SIGINT flips the
// shared admission gate from accepting to draining: business routes stop
// granting leases (post-barrier requests get a stable 503) while requests
// that already hold a lease run to completion. Once the in-flight count
// reaches zero the HTTP server is shut down exactly once and the process
// exits 0. If the leases do not drain within DRAIN_TIMEOUT the server is
// force-closed and the process exits non-zero. Repeated signals during the
// drain are absorbed: they cause neither a second shutdown nor a deadlock.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"raincut/internal/api"
	"raincut/internal/lifecycle"
)

// defaultDrainTimeout bounds the wait for in-flight leases when DRAIN_TIMEOUT
// is not set. It stays below the default docker stop grace period.
const defaultDrainTimeout = 5 * time.Second

func main() {
	// API_PORT is the user-facing knob documented in README/docker-compose;
	// PORT stays supported for backwards compatibility.
	port := os.Getenv("API_PORT")
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		port = "8080"
	}
	drainTimeout := drainTimeoutFromEnv()
	gate := lifecycle.NewGate()
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.NewMuxWithGate(gate),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Printf("raincut api listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stop
	// Absorb repeated signals so they neither interrupt the drain nor pile
	// up in the notifier; the drain sequence below runs exactly once.
	go func() {
		for extra := range stop {
			log.Printf("ignoring repeated %s during drain", extra)
		}
	}()
	log.Printf("received %s: draining in-flight requests (timeout %s)", sig, drainTimeout)
	gate.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := gate.WaitDrained(ctx); err != nil {
		log.Printf("drain did not finish within %s: forcing close", drainTimeout)
		if cerr := srv.Close(); cerr != nil {
			log.Printf("force close error: %v", cerr)
		}
		gate.MarkStopped()
		os.Exit(1)
	}
	// Every pre-barrier lease has been released: shut the server down
	// exactly once and exit successfully.
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("shutdown error: %v", err)
		gate.MarkStopped()
		os.Exit(1)
	}
	gate.MarkStopped()
	log.Printf("drained and stopped")
}

// drainTimeoutFromEnv parses DRAIN_TIMEOUT as a Go duration ("2s",
// "500ms") or, for convenience, a bare number of seconds. Unset or invalid
// values fall back to defaultDrainTimeout.
func drainTimeoutFromEnv() time.Duration {
	v := os.Getenv("DRAIN_TIMEOUT")
	if v == "" {
		return defaultDrainTimeout
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	log.Printf("invalid DRAIN_TIMEOUT %q, using %s", v, defaultDrainTimeout)
	return defaultDrainTimeout
}
