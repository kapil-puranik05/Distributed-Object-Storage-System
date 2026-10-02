package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"storage/internal/shared"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	n.sentList = loadSentList()
	for _, entry := range n.sentList {
		if entry.SequenceNumber > n.sequenceCounter {
			n.sequenceCounter = entry.SequenceNumber
		}
	}
	log.Println("Node initialized successfully")
}

func (n *Node) sendRegistrationRequest() error {
	log.Println("Sending registration request to the master")
	payload := &shared.NodeMetaDataDto{
		NodeId:  n.nodeId,
		Address: n.address,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode registration: %w", err)
	}
	url := fmt.Sprintf("http://%s/register", n.masterAddress)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("send registration: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master rejected registration with status %d", resp.StatusCode)
	}
	log.Println("Node registered successfully")
	return nil
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
	log.Printf("Received epoch=%d Current epoch=%d", req.Epoch, epoch)
	if req.Epoch != epoch {
		return false, errors.New("Stale Epoch")
	}
	if role == shared.RoleOrphan {
		return false, errors.New("node is not part of an active chain")
	}
	if role == shared.RoleHead {
		req.SequenceNumber = atomic.AddUint64(&n.sequenceCounter, 1)
	}
	switch req.Command {
	case "SET":
		log.Println("Writing chunk to disk")
		if err := os.MkdirAll(filepath.Join(n.nodeId, req.ObjectID), 0755); err != nil {
			return false, err
		}
		file := strconv.FormatUint(req.ChunkId, 10)
		path := filepath.Join(n.nodeId, req.ObjectID, file)
		if err := os.WriteFile(path, req.Data, 0644); err != nil {
			return false, err
		}
	case "DELETE":
		log.Println("Deleting object from disk")
		dir := filepath.Join(n.nodeId, req.ObjectID)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			log.Printf("Object %s already deleted", req.ObjectID)
		} else if err != nil {
			return false, err
		} else if err := os.RemoveAll(dir); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("Invalid command")
	}
	if role == shared.RoleHead || role == shared.RoleMiddle {
		entry := &LogEntry{
			SequenceNumber: req.SequenceNumber,
			WriteRequest:   req,
		}
		if err := n.appendLogEntry(entry); err != nil {
			return false, err
		}
		body, err := json.Marshal(req)
		if err != nil {
			return false, err
		}
		url := fmt.Sprintf("http://%s/write", nextAddress)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if err != nil {
			return false, fmt.Errorf("Failed forwarding write downstream to %s: %w", nextAddress, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, fmt.Errorf("Downstream node %s returned status %d", nextAddress, resp.StatusCode)
		}
		log.Println("Replication successful")
		return true, nil
	}
	log.Println("Reached tail. Sending ACK upstream")
	ackReq := shared.AckRequest{
		Epoch:          req.Epoch,
		SequenceNumber: req.SequenceNumber,
	}
	go func(addr string, payload shared.AckRequest) {
		if addr == "" {
			return
		}
		body, _ := json.Marshal(payload)
		url := fmt.Sprintf("http://%s/acknowledge", addr)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if err == nil {
			resp.Body.Close()
		}
	}(prevAddress, ackReq)
	return true, nil
}

func (n *Node) read(dir string, encoder *json.Encoder, flusher http.Flusher) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("Error occurred while opening object directory: %v", err)
	}
	defer d.Close()
	for {
		entries, err := d.ReadDir(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Error occurred while reading chunk: %v", err)
		}
		for _, entry := range entries {
			val, err := strconv.ParseUint(entry.Name(), 10, 64)
			if err != nil {
				return fmt.Errorf("Error occurred while parsing chunk Id %s: %v", entry.Name(), err)
			}
			path := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("Error occurred while reading chunk %s: %v", entry.Name(), err)
			}
			chunk := shared.Chunk{
				ID:   val,
				Data: data,
			}
			if err := encoder.Encode(chunk); err != nil {
				return fmt.Errorf("Error occurred while encoding chunk %s: %v", entry.Name(), err)
			}
			flusher.Flush()
		}
	}
	return nil
}

func (n *Node) acknowledge(req shared.AckRequest) error {
	n.configurationMutex.RLock()
	epoch := n.currentEpoch
	prevAddress := n.prevAddress
	n.configurationMutex.RUnlock()
	if req.Epoch != epoch {
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
	if cmd.SyncFromAddress != "" {
		if err := n.syncFrom(cmd.SyncFromAddress); err != nil {
			return err
		}
	}
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

func (n *Node) syncFrom(sourceAddress string) error {
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Get(fmt.Sprintf("http://%s/snapshot", sourceAddress))
	if err != nil {
		return fmt.Errorf("fetch snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot source returned status %d", resp.StatusCode)
	}
	decoder := json.NewDecoder(resp.Body)
	for {
		var chunk shared.SnapshotChunk
		if err := decoder.Decode(&chunk); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		dir := filepath.Join(n.nodeId, chunk.ObjectID)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, strconv.FormatUint(chunk.ChunkID, 10)), chunk.Data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) replayPendingWrites() error {
	n.configurationMutex.RLock()
	role, nextAddress := n.Role, n.nextAddress
	n.configurationMutex.RUnlock()
	if role != shared.RoleHead && role != shared.RoleMiddle {
		return nil
	}
	n.sentListMutex.RLock()
	pending := append([]LogEntry(nil), n.sentList...)
	n.sentListMutex.RUnlock()
	for _, entry := range pending {
		body, err := json.Marshal(entry.WriteRequest)
		if err != nil {
			return err
		}
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(fmt.Sprintf("http://%s/write", nextAddress), "application/json", bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("replay returned status %d", resp.StatusCode)
		}
	}
	return nil
}

func clearDiskLog() error {
	err := os.WriteFile(filePath, []byte{}, 0644)
	if err != nil {
		return fmt.Errorf("Failed to clear transit log while transitioning to tail")
	}
	return nil
}
