package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"router/internal/database"
	"router/internal/metadata"
	"router/internal/registry"
	"router/internal/repositories"
	"time"

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

type RetrievalInitializationRequest struct {
	Key string `json:"key"`
}

type DeleteInitializationRequest struct {
	Key string `json:"key"`
}

type DeleteInitializationResponse struct {
	ObjectId string            `json:"objectId"`
	Chains   []*registry.Chain `json:"chains"`
}

type RetrievalInitializationResponse struct {
	ObjectId       string            `json:"objectId"`
	Chains         []*registry.Chain `json:"chains"`
	NumberOfChunks uint64            `json:"numChunks"`
}

type UploadCompleteNotification struct {
	ObjectId string `json:"objectId"`
}

type UploadCompleteResponse struct {
	Success bool `json:"success"`
}

type DeleteCompletionNotification struct {
	ObjectId string `json:"objectId"`
}

type DeleteCompletionResponse struct {
	Success bool `json:"success"`
}

type storageChunk struct {
	ID uint64 `json:"id"`
}

type objectRequest struct {
	ObjectID string `json:"objectId"`
}

type objectExistsResponse struct {
	Exists bool `json:"exists"`
}

func expectedChunkCount(object *metadata.StorageObject) (uint64, error) {
	if object.ChunkSize <= 0 {
		return 0, fmt.Errorf("invalid chunk size")
	}
	if object.Size == 0 {
		return 0, nil
	}
	return (object.Size-1)/uint64(object.ChunkSize) + 1, nil
}

func verifyObjectChunks(object *metadata.StorageObject, chains []*registry.Chain) error {
	expected, err := expectedChunkCount(object)
	if err != nil {
		return err
	}
	if len(chains) == 0 {
		return fmt.Errorf("no active storage chains")
	}
	if expected == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, expected)
	client := &http.Client{Timeout: 30 * time.Second}
	body, err := json.Marshal(objectRequest{ObjectID: object.ID})
	if err != nil {
		return err
	}
	for _, chain := range chains {
		resp, err := client.Post(fmt.Sprintf("http://%s/read", chain.TailAddress), "application/json", bytes.NewBuffer(body))
		if err != nil {
			return fmt.Errorf("read tail %s: %w", chain.TailAddress, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("tail %s rejected verification", chain.TailAddress)
		}
		decoder := json.NewDecoder(resp.Body)
		for {
			var chunk storageChunk
			err := decoder.Decode(&chunk)
			if err != nil {
				break
			}
			if chunk.ID >= expected {
				resp.Body.Close()
				return fmt.Errorf("tail returned an invalid chunk")
			}
			seen[chunk.ID] = struct{}{}
		}
		resp.Body.Close()
	}
	if uint64(len(seen)) != expected {
		return fmt.Errorf("stored object is incomplete")
	}
	for id := uint64(0); id < expected; id++ {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("stored object is incomplete")
		}
	}
	return nil
}

func verifyObjectAbsent(objectID string, chains []*registry.Chain) error {
	if len(chains) == 0 {
		return fmt.Errorf("no active storage chains")
	}
	body, err := json.Marshal(objectRequest{ObjectID: objectID})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, chain := range chains {
		resp, err := client.Post(fmt.Sprintf("http://%s/exists", chain.TailAddress), "application/json", bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("tail %s rejected delete verification", chain.TailAddress)
		}
		var result objectExistsResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if result.Exists {
			return fmt.Errorf("object still exists on tail %s", chain.TailAddress)
		}
	}
	return nil
}

func UploadInitializationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req UploadIniitializationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The upload request payload is invalid")
		return
	}
	if req.ChunkSize <= 0 {
		writeError(w, http.StatusBadRequest, "The chunk size must be positive")
		return
	}
	topology := reg.CopyTopology()
	if len(topology.Chains) == 0 {
		writeError(w, http.StatusServiceUnavailable, "No active storage chain is available")
		return
	}
	object := &metadata.StorageObject{
		ID:        uuid.NewString(),
		Key:       req.Key,
		Size:      req.Size,
		ChunkSize: req.ChunkSize,
		Status:    metadata.ObjectUploading,
	}
	if err := repo.Create(object); err != nil {
		log.Printf("Error occurred while saving the object metadata: %v", err)
		writeError(w, http.StatusInternalServerError, "The router could not create metadata for this object")
		return
	}
	response := &UploadInitializationResponse{
		ObjectId: object.ID,
		Chains:   topology.Chains,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not send the upload response")
		return
	}
}

func UploadCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req UploadCompleteNotification
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The upload completion payload is invalid")
		return
	}
	object, err := repo.FindByID(req.ObjectId)
	if err != nil {
		writeError(w, http.StatusNotFound, "The uploaded object could not be found")
		return
	}
	if object.Status != metadata.ObjectUploading {
		writeError(w, http.StatusConflict, "The object is not awaiting upload completion")
		return
	}
	if err := verifyObjectChunks(object, reg.CopyTopology().Chains); err != nil {
		writeError(w, http.StatusConflict, "The uploaded object has not been fully replicated")
		return
	}
	object.Status = metadata.ObjectReady
	if err := repo.Update(object); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not commit upload completion")
		return
	}
	response := UploadCompleteResponse{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not confirm upload completion")
		return
	}
}

func RetrievalInitializationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req RetrievalInitializationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The retrieval request payload is invalid")
		return
	}
	obj, err := repo.FindByKey(req.Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "No object was found for the requested key")
		return
	}
	if obj.Status != metadata.ObjectReady {
		writeError(w, http.StatusNotFound, "The requested object is not available for retrieval")
		return
	}
	topology := reg.CopyTopology()
	numChunks, err := expectedChunkCount(obj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "The stored object metadata is invalid")
		return
	}
	response := &RetrievalInitializationResponse{
		ObjectId:       obj.ID,
		Chains:         topology.Chains,
		NumberOfChunks: numChunks,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not send the retrieval response")
		return
	}
}

func DeleteInitializationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req DeleteInitializationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The delete request payload is invalid")
		return
	}
	obj, err := repo.FindByKey(req.Key)
	if err != nil {
		writeError(w, http.StatusNotFound, "No object was found for the requested key")
		return
	}
	if obj.Status != metadata.ObjectReady {
		writeError(w, http.StatusNotFound, "The requested object is not available for deletion")
		return
	}
	topology := reg.CopyTopology()
	response := &DeleteInitializationResponse{
		ObjectId: obj.ID,
		Chains:   topology.Chains,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not send the delete response")
		return
	}
}

func DeleteCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req DeleteCompletionNotification
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The delete completion payload is invalid")
		return
	}
	object, err := repo.FindByID(req.ObjectId)
	if err != nil {
		writeError(w, http.StatusNotFound, "The object scheduled for deletion could not be found")
		return
	}
	if err := verifyObjectAbsent(object.ID, reg.CopyTopology().Chains); err != nil {
		writeError(w, http.StatusConflict, "The object still exists in storage")
		return
	}
	if err := repo.Delete(object.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not remove the object metadata")
		return
	}
	response := &DeleteCompletionResponse{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not confirm delete completion")
		return
	}
}

func ChainRegistrationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "This endpoint only accepts POST requests")
		return
	}
	var req registry.ChainRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "The chain registration payload is invalid")
		return
	}
	reg.RegisterChain(req)
	response := &ChainRegistrationResponse{
		Registered: true,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		writeError(w, http.StatusInternalServerError, "The router could not confirm chain registration")
		return
	}
}
