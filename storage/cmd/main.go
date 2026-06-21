package main

import (
	"fmt"
	"log"
	"net/http"
	"storage/internal/node"

	"github.com/robfig/cron/v3"
)

func main() {
	node.InitializeNode()
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/10 * * * * *", func() {
		node.SendHeartbeat()
	})
	if err != nil {
		log.Fatal(err)
	}
	c.Start()
	port := fmt.Sprintf(":%s", node.Address[len(node.Address)-4:])
	mux := http.NewServeMux()
	mux.HandleFunc("/configure", node.NodeReconfigurationHandler)
	mux.HandleFunc("/write", node.WriteHandler)
	mux.HandleFunc("/acknowledge", node.AcknowlegementHandler)
	if err := http.ListenAndServe(port, mux); err != nil {
		panic(err)
	}
}
