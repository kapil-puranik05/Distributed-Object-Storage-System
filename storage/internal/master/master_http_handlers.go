package master

import (
	"encoding/json"
	"net/http"
	"storage/internal/shared"
)

var (
	globalClusterLayout = &ClusterLayout{}
)

type LayoutDto struct {
	HeadAddress string `json:"headAddress"`
	TailAddress string `json:"tailAddress"`
}

func (m *MasterNodeRegistry) HandleRegisterNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var dto shared.NodeMetaDataDto
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.registerNode(&dto)
	w.WriteHeader(http.StatusOK)
}

func (m *MasterNodeRegistry) HandleGetLayout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	globalClusterLayout.LayoutMutex.RLock()
	defer globalClusterLayout.LayoutMutex.RUnlock()
	payload := &LayoutDto{
		HeadAddress: globalClusterLayout.HeadAddress,
		TailAddress: globalClusterLayout.TailAddress,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

func (m *MasterNodeRegistry) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var dto shared.NodeMetaDataDto
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err := m.updateLastSeen(dto)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}
