package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"storage/internal/master"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &master.MasterNodeRegistry{}
	m.InitializeMaster()
	m.StartHealthCheckLoop(ctx, 10*time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/register", m.HandleRegisterNode)
	mux.HandleFunc("/layout", m.HandleGetLayout)
	mux.HandleFunc("/heartbeat", m.HandleHeartbeat)
	server := &http.Server{
		Addr:    m.Address,
		Handler: mux,
	}
	go func() {
		log.Printf("Master HTTP Server listening on %s...\n", m.Address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server listen failed: %v", err)
		}
	}()
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
