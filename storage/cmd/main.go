package main

import (
	"fmt"
	"net/http"
	"storage/internal/node"
)

func main() {
	node.InitializeNode()
	port := fmt.Sprintf(":%s", node.Address[len(node.Address)-4:])
	mux := http.NewServeMux()
	mux.HandleFunc("/configure", node.NodeReconfigurationHandler)
	mux.HandleFunc("/write", node.WriteHandler)
	mux.HandleFunc("/acknowledge", node.AcknowlegementHandler)
	if err := http.ListenAndServe(port, mux); err != nil {
		panic(err)
	}
}
