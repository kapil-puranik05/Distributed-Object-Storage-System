package handlers

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"sync"
)

var (
	handler = &FileHandler{
		UploadDir: "./tmp_chunks",
	}
	uploadRegistry = make(map[string][]FilePart)
	registryMutex  sync.Mutex
)

func ReceiveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		http.Error(w, "Missing 'upload_id' query parameter", http.StatusBadRequest)
		return
	}
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Invalid multipart request: "+err.Error(), http.StatusBadRequest)
		return
	}
	parts, err := handler.ReceiveMultipartFile(reader)
	if err != nil {
		http.Error(w, "Failed to save chunks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	registryMutex.Lock()
	uploadRegistry[uploadID] = append(uploadRegistry[uploadID], parts...)
	registryMutex.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "success",
		"chunks_saved": len(parts),
	})
}

func AssembleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		http.Error(w, "Missing 'upload_id' query parameter", http.StatusBadRequest)
		return
	}
	registryMutex.Lock()
	parts, exists := uploadRegistry[uploadID]
	if exists {
		delete(uploadRegistry, uploadID)
	}
	registryMutex.Unlock()
	if !exists || len(parts) == 0 {
		http.Error(w, "No parts found for the given upload_id", http.StatusNotFound)
		return
	}
	finalFileName := "assembled_" + parts[0].PartFileName
	finalOutputPath := filepath.Join(handler.UploadDir, finalFileName)
	err := handler.AssembleMultipartFile(parts, finalOutputPath)
	if err != nil {
		http.Error(w, "Failed to assemble file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "assembled",
		"file_path": finalOutputPath,
	})
}
