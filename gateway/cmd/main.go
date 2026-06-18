package main

import (
	"gateway/internal/handlers"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/upload/receive", handlers.ReceiveHandler)
	mux.HandleFunc("/upload/assemble", handlers.AssembleHandler)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		panic(err)
	}
}
