package metadata

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Object struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()"`
	BucketName  string    `gorm:"type:varchar(63);not null;index:idx_bucket_filename,priority:1"`
	Filename    string    `gorm:"type:varchar(1024);not null;index:idx_bucket_filename,priority:2"`
	TotalSize   int64     `gorm:"not null;check:total_size >= 0"`
	ContentType string    `gorm:"type:varchar(255);not null;default:'application/octet-stream'"`
	Status      string    `gorm:"type:varchar(50);not null;default:'UPLOADING'"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt `gorm:"index"`
	Parts       []ObjectPart   `gorm:"foreignKey:ParentObjectRefer;constraint:OnDelete:CASCADE;"`
}

type ObjectPart struct {
	ID                uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()"`
	ParentObjectRefer uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_parent_part,priority:1"`
	PartNumber        int       `gorm:"not null;uniqueIndex:idx_parent_part,priority:2;check:part_number > 0"`
	ChunkSize         int       `gorm:"not null;check:chunk_size > 0"`
	ChainID           string    `gorm:"type:varchar(128);not null"`
	Checksum          string    `gorm:"type:varchar(64);not null"`
	CreatedAt         time.Time
}
