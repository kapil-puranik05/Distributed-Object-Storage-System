package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"storage/internal/shared"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	basePath string = os.Getenv("NODE_PATH")
	nodeFile string = fmt.Sprintf("%s/node.json", basePath)
	filePath string = fmt.Sprintf("%s/log.txt", basePath)
)

type LogEntry struct {
	SequenceNumber uint64              `json:"sequenceNumber"`
	WriteRequest   shared.WriteRequest `json:"writeRequest"`
}

type Node struct {
	configurationMutex sync.RWMutex
	currentEpoch       uint64
	Role               shared.Role
	prevAddress        string
	nextAddress        string
	sequenceCounter    uint64
	sentListMutex      sync.RWMutex
	sentList           []LogEntry
	address            string
	masterAddress      string
	nodeId             string
}

func (n *Node) initializeNode() {
	data, err := os.ReadFile(nodeFile)
	if err != nil {
		log.Fatalf("Error occurred while reading node state: %v", err)
	}
	var result map[string]string
	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Fatalf("Error occurred while parsing node state: %v", err)
	}
	log.Println("Node state read from disk")
	n.address = result["address"]
	n.masterAddress = result["masterAddress"]
	n.nodeId = result["nodeId"]
	n.currentEpoch = 0
	n.Role = shared.RoleOrphan
	n.sequenceCounter = 0
	n.sentList = loadSentList()
	log.Println("Node initialized successfully")
}

func (n *Node) sendRegistrationRequest() {
	log.Println("Sending registration request to the master")
	payload := &shared.NodeMetaDataDto{
		NodeId:  n.nodeId,
		Address: n.address,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error occurred while encoding node data for node registration: %v", err)
	}
	url := fmt.Sprintf("http://%s/register", n.masterAddress)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error occurred while sending registration request to master: %v", err)
	}
	resp.Body.Close()
	log.Println("Node registered successfully")
}

func loadSentList() []LogEntry {
	var sentList []LogEntry
	file, err := os.Open(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sentList
		}
		log.Fatalf("Error occurred while opening the log: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		data := scanner.Bytes()
		err := json.Unmarshal(data, &entry)
		if err != nil {
			log.Fatalf("Error occurred while parsing the log entry: %v", err)
		}
		sentList = append(sentList, entry)
	}
	if err = scanner.Err(); err != nil {
		log.Fatalf("Error occurred while parsing the log: %v", err)
	}
	return sentList
}

func (n *Node) appendLogEntry(entry *LogEntry) error {
	n.sentListMutex.Lock()
	n.sentList = append(n.sentList, *entry)
	n.sentListMutex.Unlock()
	data, err := json.Marshal(&entry)
	if err != nil {
		return fmt.Errorf("Error occurred while appending the log entry: %w", err)
	}
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Error occurred while opening the file for appending: %w", err)
	}
	defer file.Close()
	_, err = file.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("Failed to write the entry to the log: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("Failed to sync log to disk: %w", err)
	}
	return nil
}

func (n *Node) write(req shared.WriteRequest) (bool, error) {
	n.configurationMutex.RLock()
	epoch := n.currentEpoch
	role := n.Role
	nextAddress := n.nextAddress
	prevAddress := n.prevAddress
	n.configurationMutex.RUnlock()
	log.Printf("Received epoch=%d Current epoch=%d", req.Epoch, n.currentEpoch)
	if req.Epoch < epoch {
		return false, errors.New("Stale Epoch")
	}
	if role == shared.RoleHead {
		newSequenceNumber := atomic.AddUint64(&n.sequenceCounter, 1)
		req.SequenceNumber = newSequenceNumber
	}
	log.Println("Writing data to disk")
	if err := os.MkdirAll(filepath.Join(n.nodeId, req.ObjectID), 0755); err != nil {
		log.Printf("Error occurred while creating object group: %v", err)
		return false, err
	}
	file := strconv.FormatUint(req.ChunkId, 10)
	path := filepath.Join(n.nodeId, req.ObjectID, file)
	if err := os.WriteFile(path, req.Data, 0644); err != nil {
		log.Printf("Error occurred while writing chunk %d to the disk: %v", req.ChunkId, err)
		return false, err
	}
	log.Println("Data written to disk successfully")
	if role == shared.RoleHead || role == shared.RoleMiddle {
		log.Println("Replicating data to successor node")
		entry := &LogEntry{
			SequenceNumber: req.SequenceNumber,
			WriteRequest:   req,
		}
		err := n.appendLogEntry(entry)
		if err != nil {
			return false, err
		}
		jsonData, err := json.Marshal(req)
		if err != nil {
			return false, fmt.Errorf("failed to marshal forward write payload: %w", err)
		}
		url := fmt.Sprintf("http://%s/write", nextAddress)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			_ = os.Remove(path)
			return false, fmt.Errorf("Failed forwarding write downstream to %s: %w", nextAddress, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			_ = os.Remove(path)
			return false, fmt.Errorf("Downstream node %s returned status: %d", nextAddress, resp.StatusCode)
		}
	} else if role == shared.RoleTail {
		log.Println("Data replicated to tail node")
		ackReq := shared.AckRequest{
			Epoch:          epoch,
			SequenceNumber: req.SequenceNumber,
		}
		go func(addr string, payload shared.AckRequest) {
			if addr == "" {
				return
			}
			jsonData, _ := json.Marshal(payload)
			url := fmt.Sprintf("http://%s/acknowledge", addr)
			_, _ = http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		}(prevAddress, ackReq)
		return true, nil
	}
	return true, nil
}

func (n *Node) acknowledge(req shared.AckRequest) error {
	n.configurationMutex.RLock()
	epoch := n.currentEpoch
	prevAddress := n.prevAddress
	n.configurationMutex.RUnlock()
	if req.Epoch < epoch {
		return errors.New("Stale Epoch")
	}
	n.sentListMutex.Lock()
	targetIndex := -1
	for i, entry := range n.sentList {
		if entry.SequenceNumber == req.SequenceNumber {
			targetIndex = i
			break
		}
	}
	if targetIndex != -1 {
		n.sentList = append(n.sentList[:targetIndex], n.sentList[targetIndex+1:]...)
	}
	n.sentListMutex.Unlock()
	if targetIndex != -1 {
		if err := n.rewriteDiskLog(); err != nil {
			return fmt.Errorf("Error occurred while compacting the log")
		}
	}
	log.Println("Acknowledgement received from successor node")
	if prevAddress != "" {
		go func(addr string, payload shared.AckRequest) {
			jsonData, err := json.Marshal(payload)
			if err != nil {
				return
			}
			url := fmt.Sprintf("http://%s/acknowledge", addr)
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err == nil {
				resp.Body.Close()
			}
		}(prevAddress, req)
	}
	return nil
}

func (n *Node) rewriteDiskLog() error {
	n.sentListMutex.RLock()
	defer n.sentListMutex.RUnlock()
	tempPath := fmt.Sprintf("%s.tmp", filePath)
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	for _, entry := range n.sentList {
		data, err := json.Marshal(&entry)
		if err != nil {
			file.Close()
			return err
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	file.Close()
	return os.Rename(tempPath, filePath)
}

func (n *Node) reconfigure(cmd shared.ReConfigCommand) error {
	n.configurationMutex.Lock()
	defer n.configurationMutex.Unlock()
	if cmd.NewEpoch <= n.currentEpoch {
		return errors.New("Stale Epoch")
	}
	n.currentEpoch = cmd.NewEpoch
	n.Role = cmd.AssignedRole
	n.prevAddress = cmd.PrevAddress
	n.nextAddress = cmd.NextAddress
	if n.Role == shared.RoleTail {
		n.sentListMutex.Lock()
		n.sentList = nil
		n.sentListMutex.Unlock()
		log.Printf("Node reconfigured successfully to Role: %s", shared.RoleTail)
		return clearDiskLog()
	}
	log.Printf("Node reconfigured successfully to Role: %s", n.Role)
	return nil
}

func clearDiskLog() error {
	err := os.WriteFile(filePath, []byte{}, 0644)
	if err != nil {
		return fmt.Errorf("Failed to clear transit log while transitioning to tail")
	}
	return nil
}
