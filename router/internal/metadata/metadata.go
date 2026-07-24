package metadata

import "time"

type ObjectStatus string

const (
	ObjectUploading ObjectStatus = "UPLOADING"
	ObjectReady     ObjectStatus = "READY"
	ObjectDeleted   ObjectStatus = "DELETED"
)

type StorageObject struct {
	ID        string       `gorm:"primaryKey;size:36"`
	Key       string       `gorm:"not null;uniqueIndex:idx_object_key"`
	Size      uint64       `gorm:"not null"`
	ChunkSize int64        `gorm:"not null"`
	Status    ObjectStatus `gorm:"type:varchar(20);not null"`
	LastChunk int64        `gorm:"not null"`
	CreatedAt time.Time
	UpdatedAt time.Time
}
