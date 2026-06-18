package handlers

import (
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"os"
	"path/filepath"
)

type FileHandler struct {
	UploadDir string
}

type FilePart struct {
	PartForm     string
	PartFileName string
	DiskPath     string
}

func (fh *FileHandler) AssembleMultipartFile(parts []FilePart, finalOutputPath string) error {
	if len(parts) == 0 {
		return fmt.Errorf("No parts Provided for assembly")
	}
	finalFile, err := os.Create(finalOutputPath)
	if err != nil {
		log.Println("Error creating assembled file: ", err)
		return err
	}
	defer finalFile.Close()
	for _, part := range parts {
		err := func() error {
			chunkFile, err := os.Open(part.DiskPath)
			if err != nil {
				return err
			}
			defer chunkFile.Close()
			_, err = io.Copy(finalFile, chunkFile)
			return err
		}()
		if err != nil {
			log.Printf("Error merging chunk %s: %v\n", part.DiskPath, err)
			return err
		}
	}
	for _, part := range parts {
		_ = os.Remove(part.DiskPath)
	}
	return nil
}

func (fh *FileHandler) ReceiveMultipartFile(reader *multipart.Reader) ([]FilePart, error) {
	var parts []FilePart
	if err := os.MkdirAll(fh.UploadDir, os.ModePerm); err != nil {
		return nil, err
	}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Error occurred while processing the part")
			return nil, err
		}
		if part.FileName() == "" {
			continue
		}
		destinationPath := filepath.Join(fh.UploadDir, part.FileName())
		destinationFile, err := os.Create(destinationPath)
		if err != nil {
			log.Println("Error creating local file chunk: ", err)
			part.Close()
			return nil, err
		}
		_, err = io.Copy(destinationFile, part)
		destinationFile.Close()
		part.Close()
		if err != nil {
			log.Println("Error occurred while writing the chunk to disk")
			return nil, err
		}
		filepart := FilePart{
			PartForm:     part.FormName(),
			PartFileName: part.FileName(),
			DiskPath:     destinationPath,
		}
		parts = append(parts, filepart)
	}
	return parts, nil
}
