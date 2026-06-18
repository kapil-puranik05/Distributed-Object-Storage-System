package node

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Node struct {
	Host     string
	Port     uint64
	Topology []uint64
	Role     string
}

type NodeDetails struct {
	Host     string   `json:"host"`
	Port     uint64   `json:"port"`
	Topology []uint64 `json:"topology"`
}

func (n *Node) InitializeNode() {
	basePath := os.Getenv("NODE_PATH")
	if basePath == "" {
		basePath = "."
	}
	data, err := os.ReadFile(fmt.Sprintf("%s/node.json", basePath))
	if err != nil {
		log.Printf("Error occurred while reading the node properties: %v", err)
		return
	}
	var nodeDetails NodeDetails
	err = json.Unmarshal(data, &nodeDetails)
	if err != nil {
		log.Printf("Error occurred while parsing the node properties: %v", err)
		return
	}
	n.Host = nodeDetails.Host
	n.Port = nodeDetails.Port
	n.Topology = nodeDetails.Topology
	size := len(n.Topology)
	if size == 0 {
		log.Fatal("Topology is undefined, role cannot be determined")
	}
	switch true {
	case n.Port == n.Topology[0]:
		n.Role = "HEAD"
	case n.Port == n.Topology[size-1]:
		n.Role = "TAIL"
	default:
		found := false
		for _, port := range n.Topology {
			if n.Port == port {
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("Node port %d not found in topology %v", n.Port, n.Topology)
		}
		n.Role = "MIDDLE"
	}
	log.Printf("Successfully initialized the node as %s", n.Role)
}
