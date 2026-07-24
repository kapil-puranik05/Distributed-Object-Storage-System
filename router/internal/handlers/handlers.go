package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"router/internal/database"
	"router/internal/metadata"
	"router/internal/registry"
	"router/internal/repositories"

	"github.com/google/uuid"
)

var (
	repo *repositories.StorageObjectRepository
	reg  = &registry.Registry{}
)

func Init() {
	reg.InitializeRegistry()
	repo = repositories.NewStorageObjectRepository(database.DB)
}

type UploadIniitializationRequest struct {
	Key       string `json:"key"`
	Size      uint64 `json:"size"`
	ChunkSize int64  `json:"chunkSize"`
}

type UploadInitializationResponse struct {
	ObjectId string            `json:"objectId"`
	Chains   []*registry.Chain `json:"chains"`
}

type ChainRegistrationResponse struct {
	Registered bool `json:"registered"`
}

func UploadInitializationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Error: Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req UploadIniitializationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Error: Unable to parse the request", http.StatusBadRequest)
		return
	}
	object := &metadata.StorageObject{
		ID:        uuid.NewString(),
		Key:       req.Key,
		Size:      req.Size,
		ChunkSize: req.ChunkSize,
		Status:    metadata.ObjectUploading,
		LastChunk: -1,
	}
	if err := repo.Create(object); err != nil {
		log.Printf("Error occurred while saving the object metadata: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	topology := reg.CopyTopology()
	response := &UploadInitializationResponse{
		ObjectId: object.ID,
		Chains:   topology.Chains,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ChainRegistrationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Error: Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req registry.ChainRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Error: Unable to parse the request", http.StatusBadRequest)
		return
	}
	reg.RegisterChain(req)
	response := &ChainRegistrationResponse{
		Registered: true,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
