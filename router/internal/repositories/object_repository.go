package repositories

import (
	"router/internal/metadata"

	"gorm.io/gorm"
)

type StorageObjectRepository struct {
	db *gorm.DB
}

func NewStorageObjectRepository(db *gorm.DB) *StorageObjectRepository {
	return &StorageObjectRepository{
		db: db,
	}
}

func (r *StorageObjectRepository) Create(object *metadata.StorageObject) error {
	return r.db.Create(object).Error
}

func (r *StorageObjectRepository) FindByID(id string) (*metadata.StorageObject, error) {
	var object metadata.StorageObject
	if err := r.db.First(&object, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &object, nil
}

func (r *StorageObjectRepository) FindByKey(key string) (*metadata.StorageObject, error) {
	var object metadata.StorageObject
	if err := r.db.First(&object, "key = ?", key).Error; err != nil {
		return nil, err
	}
	return &object, nil
}

func (r *StorageObjectRepository) FindAll() ([]metadata.StorageObject, error) {
	var objects []metadata.StorageObject
	if err := r.db.Find(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}

func (r *StorageObjectRepository) Update(object *metadata.StorageObject) error {
	return r.db.Save(object).Error
}

func (r *StorageObjectRepository) Delete(id string) error {
	return r.db.Delete(&metadata.StorageObject{}, "id = ?", id).Error
}
