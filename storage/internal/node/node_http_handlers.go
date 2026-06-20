package node

import (
	"encoding/json"
	"net/http"
	"storage/internal/shared"
)

var (
	node    = &Node{}
	Address string
)

type WriteResponse struct {
	IsWritten bool
}

type NodeReconfigurationResponse struct {
	Message    string `json:"message"`
	StatusCode int    `json:"statusCode"`
}

func InitializeNode() {
	node.initializeNode()
	Address = node.address
}

func NodeReconfigurationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Error: Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var cmd shared.ReConfigCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Error: Unable to parse the request", http.StatusBadRequest)
		return
	}
	if err := node.reconfigure(cmd); err != nil {
		if err.Error() == "Stale Epoch" {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := &NodeReconfigurationResponse{
		Message:    "Node reconfigured successfully",
		StatusCode: http.StatusOK,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func WriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Error: Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req shared.WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Error: Unable to parse the request", http.StatusBadRequest)
		return
	}
	val, err := node.write(req)
	if err != nil {
		if err.Error() == "Stale Epoch" {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := &WriteResponse{
		IsWritten: val,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func AcknowlegementHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Error: Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req shared.AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Error: Unable to parse the request", http.StatusBadRequest)
		return
	}
	if err := node.acknowledge(req); err != nil {
		if err.Error() == "Stale Epoch" {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
