package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

const (
	bufferSize int = 64 * 1024 * 1024
)

type UploadRequest struct {
	Key       string `json:"key"`
	Size      uint64 `json:"size"`
	ChunkSize int64  `json:"chunkSize"`
}

type UploadResponse struct {
	ObjectId string   `json:"objectId"`
	Chains   []*Chain `json:"chains"`
}

type Chain struct {
	HeadAddress   string `json:"headAddress"`
	TailAddress   string `json:"tailAddress"`
	MasterAddress string `json:"masterAddress"`
	ChainId       string `json:"chainId"`
	Epoch         uint64 `json:"epoch"`
}

type Chunk struct {
	ID   uint64
	Data []byte
}

type ChunkUploadRequest struct {
	Epoch          uint64 `json:"epoch"`
	SequenceNumber uint64 `json:"sequenceNumber"`
	ObjectId       string `json:"objectId"`
	Data           []byte `json:"data"`
	ChunkId        uint64 `json:"chunkId"`
}

type Config struct {
	Epoch uint64 `json:"epoch"`
}

type ChunkUploadResponse struct {
	IsWritten bool `json:"isWritten"`
}

func ProcessFileInChunks(filename string, bufferSize int, processor func(Chunk) error) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Failed to open the file: %v", err)
	}
	defer file.Close()
	buffer := make([]byte, bufferSize)
	var chunkId uint64 = 0
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buffer[:n])
			chunk := Chunk{
				ID:   chunkId,
				Data: data,
			}
			if err := processor(chunk); err != nil {
				return err
			}
			chunkId++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Failed to read the file: %v", err)
		}
	}
	return nil
}

func GetFileSize(filename string) (uint64, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func UploadFile(filename string) error {
	size, err := GetFileSize(filename)
	if err != nil {
		return fmt.Errorf("Failed to fetch file size: %v", err)
	}
	request := &UploadRequest{
		Key:  filename,
		Size: size,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("Failed to marshal metadata: %v", err)
	}
	resp, err := http.Post("http://localhost:8000/upload", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Server returned: %s", resp.Status)
	}
	var uploadResp UploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return fmt.Errorf("Failed to decode response: %v", err)
	}
	log.Printf("Chains received: %d", len(uploadResp.Chains))
	epochs := make([]uint64, 0)
	for _, chain := range uploadResp.Chains {
		configUrl := fmt.Sprintf("http://%s/layout", chain.MasterAddress)
		var epochResponse Config
		resp, er := http.Get(configUrl)
		if er != nil {
			return fmt.Errorf("Failed to GET epoch")
		}
		if er = json.NewDecoder(resp.Body).Decode(&epochResponse); er != nil {
			return fmt.Errorf("Failed to decode epoch response")
		}
		resp.Body.Close()
		epochs = append(epochs, epochResponse.Epoch)
	}
	log.Printf("Epochs fetched: %d", len(epochs))
	nextIndex := 0
	n := len(uploadResp.Chains)
	if err := ProcessFileInChunks(filename, bufferSize, func(c Chunk) error {
		chain := uploadResp.Chains[nextIndex]
		// Note: The sequence number that we use here does not represent the ID of the chunk. It represents the sequence number that the client has sent to the chain for replication.
		chunkUploadRequest := &ChunkUploadRequest{
			Epoch:          epochs[nextIndex],
			SequenceNumber: 0,
			ObjectId:       uploadResp.ObjectId,
			Data:           c.Data,
			ChunkId:        c.ID,
		}
		nextIndex = (nextIndex + 1) % n
		log.Printf("Sending chunk %d with epoch %d", c.ID, epochs[nextIndex])
		body, er := json.Marshal(chunkUploadRequest)
		if er != nil {
			return fmt.Errorf("Failed to marshal chunk: %v", err)
		}
		url := fmt.Sprintf("http://%s/write", chain.HeadAddress)
		resp, er = http.Post(url, "application/json", bytes.NewBuffer(body))
		if er != nil {
			return fmt.Errorf("Failed to upload chunk")
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("write failed: %s: %s", resp.Status, string(body))
		}
		var chunkUploadResponse ChunkUploadResponse
		if er = json.NewDecoder(resp.Body).Decode(&chunkUploadResponse); er != nil {
			log.Println(resp.Status)
			fmt.Println(er)
			resp.Body.Close()
			return fmt.Errorf("Failed to decode chunk upload response")
		}
		resp.Body.Close()
		if !chunkUploadResponse.IsWritten {
			return fmt.Errorf("Failed to write chunk %d", c.ID)
		}
		log.Printf("Chunk %d written successfully", c.ID)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func DeleteFile(filename string) {

}

func RetrieveFile() {

}
