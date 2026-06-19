package node

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"storage/internal/shared"
	"sync"
)

type LogEntry struct {
	SequenceNumber uint64              `json:"sequenceNumber"`
	WriteRequest   shared.WriteRequest `json:"writeRequest"`
}

type Node struct {
	configurationMutex sync.RWMutex
	currentEpoch       int
	Role               shared.Role
	prevAddress        string
	nextAddress        string
	sequenceCounter    uint64
	sentListMutex      sync.RWMutex
	sentList           []LogEntry
	address            string
}

func (n *Node) InitializeNode() {
	var basePath string = os.Getenv("NODE_PATH")
	var nodeFile string = fmt.Sprintf("%s/node.json", basePath)
	var filePath string = fmt.Sprintf("%s/log.txt", basePath)
	data, err := os.ReadFile(nodeFile)
	if err != nil {
		log.Fatalf("Error occurred while reading node state: %v", err)
	}
	var result map[string]string
	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Fatalf("Error occurred while parsing node state: %v", err)
	}
	n.address = result["address"]
	n.currentEpoch = 0
	n.Role = shared.RoleOrphan
	n.sequenceCounter = 0
	n.sentList = loadSentList(filePath)
}

func loadSentList(filePath string) []LogEntry {
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

func appendLogEntry(entry LogEntry, filePath string) error {
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
