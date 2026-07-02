package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"storage/internal/node"
	"syscall"
	"time"
)

func StartHeartbeatLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		log.Printf("Heartbeat background loop started with an interval of %v", interval)
		for {
			select {
			case <-ticker.C:
				node.SendHeartbeat()
			case <-ctx.Done():
				log.Println("Heartbeat loop stopped due to context cancellation")
				return
			}
		}
	}()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.InitializeNode()
	mux := http.NewServeMux()
	mux.HandleFunc("/configure", node.NodeReconfigurationHandler)
	mux.HandleFunc("/write", node.WriteHandler)
	mux.HandleFunc("/acknowledge", node.AcknowlegementHandler)
	server := &http.Server{
		Addr:    node.Address,
		Handler: mux,
	}
	go func() {
		log.Printf("Storage Node HTTP Server listening on %s...\n", node.Address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Local server listen failed: %v", err)
		}
	}()
	node.SendRegistrationRequest()
	StartHeartbeatLoop(ctx, 10*time.Second)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
}
