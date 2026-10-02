package master

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"storage/internal/shared"
	"sync"
	"time"
)

var (
	basePath string = os.Getenv("MASTER_PATH")
	filename string = "master.json"
)

type NodeTask struct {
	TargetAddress string
	Payload       shared.ReConfigCommand
}

type ClusterLayout struct {
	LayoutMutex sync.RWMutex
	Epoch       uint64 `json:"epoch"`
	HeadAddress string `json:"headAddress"`
	TailAddress string `json:"tailAddress"`
}

type MasterDetails struct {
	Address       string `json:"address"`
	ChainId       string `json:"chainId"`
	RouterAddress string `json:"routerAddress"`
}

type NodeMetaData struct {
	NodeId   string    `json:"nodeId"`
	Address  string    `json:"address"`
	LastSeen time.Time `json:"lastseen"`
}

type MasterNodeRegistry struct {
	configurationMutex sync.RWMutex
	currentEpoch       uint64
	chainLayout        []*NodeMetaData
	nodeRegistry       map[string]*NodeMetaData
	Address            string
	RouterAddress      string
	ChainId            string
}

type ChainRegistrationRequest struct {
	ChainId       string `json:"chainId"`
	HeadAddress   string `json:"headAddress"`
	TailAddress   string `json:"tailAddress"`
	MasterAddress string `json:"masterAddress"`
}

func (m *MasterNodeRegistry) InitializeMaster() {
	var masterDetails MasterDetails
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", basePath, filename))
	if err != nil {
		log.Fatalf("Error occurred while reading master configuration: %v", err)
	}
	err = json.Unmarshal(data, &masterDetails)
	if err != nil {
		log.Fatalf("Error occurred while parsing master configuration: %v", err)
	}
	m.currentEpoch = 0
	m.chainLayout = make([]*NodeMetaData, 0)
	m.nodeRegistry = make(map[string]*NodeMetaData)
	m.Address = masterDetails.Address
	m.RouterAddress = masterDetails.RouterAddress
	m.ChainId = masterDetails.ChainId
	log.Println("Master initialized successfully")
}

func (m *MasterNodeRegistry) registerNode(nodeDto *shared.NodeMetaDataDto) {
	m.configurationMutex.Lock()
	defer m.configurationMutex.Unlock()
	node, exists := m.nodeRegistry[nodeDto.NodeId]
	if !exists {
		node = &NodeMetaData{
			NodeId:  nodeDto.NodeId,
			Address: nodeDto.Address,
		}
		m.nodeRegistry[nodeDto.NodeId] = node
	}
	node.Address = nodeDto.Address
	node.LastSeen = time.Now()
	inLayout := false
	for i, n := range m.chainLayout {
		if n.NodeId == nodeDto.NodeId {
			m.chainLayout[i].Address = nodeDto.Address
			inLayout = true
			break
		}
	}
	if !inLayout {
		chainLen := len(m.chainLayout)
		if chainLen < 2 {
			m.chainLayout = append(m.chainLayout, node)
			log.Println("Node registered, waiting to meet minimum cluster size")
		} else if chainLen == 2 {
			m.chainLayout = append(m.chainLayout, node)
			log.Println("Cluster threshold met, activating epoch")
			if m.applyReconfigurationSequence(node.NodeId) {
				m.sendCurrentChainRegistration()
			}
		} else {
			m.chainLayout = append(m.chainLayout, node)
			log.Println("New Node registered successfully")
			if m.applyReconfigurationSequence(node.NodeId) {
				m.sendCurrentChainRegistration()
			}
		}
	} else {
		log.Printf("Active Node %s re-registered / recovered smoothly.\n", node.NodeId)
	}
}

func (m *MasterNodeRegistry) sendCurrentChainRegistration() {
	globalClusterLayout.LayoutMutex.RLock()
	request := ChainRegistrationRequest{
		ChainId: m.ChainId, HeadAddress: globalClusterLayout.HeadAddress,
		TailAddress: globalClusterLayout.TailAddress, MasterAddress: m.Address,
	}
	globalClusterLayout.LayoutMutex.RUnlock()
	m.sendChainRegistrationRequest(request)
}

func (m *MasterNodeRegistry) sendChainRegistrationRequest(req ChainRegistrationRequest) {
	body, err := json.Marshal(req)
	if err != nil {
		log.Printf("Failed to marshal chain registration request: %v", err)
		return
	}
	url := fmt.Sprintf("http://%s/register", m.RouterAddress)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to send chain registration request: %v", err)
		return
	}
	resp.Body.Close()
}

func (m *MasterNodeRegistry) StartHealthCheckLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		log.Printf("Health check loop started with a interval of %v", interval)
		for {
			select {
			case <-ticker.C:
				m.healthCheck()
			case <-ctx.Done():
				log.Println("Health check loop stopped due to context cancellation")
				return
			}
		}
	}()
}

// Will run in background
func (m *MasterNodeRegistry) healthCheck() {
	m.configurationMutex.Lock()
	defer m.configurationMutex.Unlock()
	if len(m.chainLayout) == 0 {
		return
	}
	failureDetected := false
	for index, n := range m.chainLayout {
		if time.Since(m.chainLayout[index].LastSeen) > 30*time.Second {
			log.Printf("Failure detected for Node: %s", n.NodeId)
			m.chainLayout = append(m.chainLayout[:index], m.chainLayout[index+1:]...)
			failureDetected = true
			break
		}
	}
	if failureDetected {
		if m.applyReconfigurationSequence("") {
			m.sendCurrentChainRegistration()
		}
	}
}

func (m *MasterNodeRegistry) applyReconfigurationSequence(syncNodeID string) bool {
	nextEpoch := m.currentEpoch + 1
	chainLength := len(m.chainLayout)
	epoch := nextEpoch
	var tasks []NodeTask
	if chainLength == 0 {
		return false
	} else if chainLength == 1 {
		targetNode := m.chainLayout[0]
		task := NodeTask{
			TargetAddress: targetNode.Address,
			Payload: shared.ReConfigCommand{
				AssignedRole: shared.RoleOrphan,
				NewEpoch:     epoch,
				PrevAddress:  "",
				NextAddress:  "",
			},
		}
		tasks = append(tasks, task)
	} else {
		for i := 0; i < chainLength; i++ {
			var cmd shared.ReConfigCommand
			cmd.NewEpoch = epoch
			if i == 0 {
				cmd.AssignedRole = shared.RoleHead
				cmd.PrevAddress = ""
				cmd.NextAddress = m.chainLayout[i+1].Address
			} else if i == chainLength-1 {
				cmd.AssignedRole = shared.RoleTail
				cmd.PrevAddress = m.chainLayout[i-1].Address
				cmd.NextAddress = ""
				if m.chainLayout[i].NodeId == syncNodeID {
					cmd.SyncFromAddress = m.chainLayout[i-1].Address
				}
			} else {
				cmd.AssignedRole = shared.RoleMiddle
				cmd.PrevAddress = m.chainLayout[i-1].Address
				cmd.NextAddress = m.chainLayout[i+1].Address
			}
			tasks = append(tasks, NodeTask{
				TargetAddress: m.chainLayout[i].Address,
				Payload:       cmd,
			})
		}
	}
	log.Println("Applying reconfiguration sequence to the active nodes")
	// Configure tail-to-head so a replay from an upstream node always has a ready successor.
	for i := len(tasks) - 1; i >= 0; i-- {
		task := tasks[i]
		data, err := json.Marshal(task.Payload)
		if err != nil {
			return false
		}
		resp, err := (&http.Client{Timeout: 30 * time.Second}).Post(fmt.Sprintf("http://%s/configure", task.TargetAddress), "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Failed to configure node at %s: %v", task.TargetAddress, err)
			return false
		}
		status := resp.StatusCode
		resp.Body.Close()
		if status != http.StatusOK {
			log.Printf("Node at %s rejected configuration with status %d", task.TargetAddress, status)
			return false
		}
	}
	m.currentEpoch = nextEpoch
	globalClusterLayout.LayoutMutex.Lock()
	globalClusterLayout.Epoch = epoch
	globalClusterLayout.HeadAddress = m.chainLayout[0].Address
	globalClusterLayout.TailAddress = m.chainLayout[chainLength-1].Address
	globalClusterLayout.LayoutMutex.Unlock()
	return true
}

func (m *MasterNodeRegistry) updateLastSeen(nodeData shared.NodeMetaDataDto) error {
	m.configurationMutex.Lock()
	defer m.configurationMutex.Unlock()
	node, exists := m.nodeRegistry[nodeData.NodeId]
	if !exists {
		return errors.New("Node with given details not found in the registry")
	}
	lastseen := time.Now()
	node.LastSeen = lastseen
	for i := range m.chainLayout {
		if m.chainLayout[i].NodeId == nodeData.NodeId {
			m.chainLayout[i].LastSeen = lastseen
			break
		}
	}
	return nil
}
