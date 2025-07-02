package bptree

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type RecordID struct {
	PageID int32 `json:"page_id"`
	SlotID int   `json:"slot_id"`
}

type SimpleIndex struct {
	tableName string
	basePath  string
	index     map[int]RecordID
	nextID    int
	mutex     sync.RWMutex
}

func NewSimpleIndex(tableName, basePath string) *SimpleIndex {
	return &SimpleIndex{
		tableName: tableName,
		basePath:  basePath,
		index:     make(map[int]RecordID),
		nextID:    1,
	}
}

func (si *SimpleIndex) Load() error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	indexPath := filepath.Join(si.basePath, si.tableName+".idx")

	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index: %v", err)
	}

	var indexData struct {
		Index  map[int]RecordID `json:"index"`
		NextID int              `json:"next_id"`
	}

	if err := json.Unmarshal(data, &indexData); err != nil {
		return fmt.Errorf("failed to unmarshal index: %v", err)
	}

	si.index = indexData.Index
	si.nextID = indexData.NextID

	return nil
}

func (si *SimpleIndex) Save() error {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	indexPath := filepath.Join(si.basePath, si.tableName+".idx")

	indexData := struct {
		Index  map[int]RecordID `json:"index"`
		NextID int              `json:"next_id"`
	}{
		Index:  si.index,
		NextID: si.nextID,
	}

	data, err := json.MarshalIndent(indexData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %v", err)
	}

	return os.WriteFile(indexPath, data, 0644)
}

func (si *SimpleIndex) Insert(rid RecordID) (int, error) {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	id := si.nextID
	si.index[id] = rid
	si.nextID++

	return id, nil
}

func (si *SimpleIndex) Search(id int) (RecordID, bool) {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	rid, exists := si.index[id]
	return rid, exists
}

func (si *SimpleIndex) Delete(id int) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if _, exists := si.index[id]; !exists {
		return fmt.Errorf("record id %d not found", id)
	}

	delete(si.index, id)
	return nil
}

func (si *SimpleIndex) Update(id int, rid RecordID) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if _, exists := si.index[id]; !exists {
		return fmt.Errorf("record id %d not found", id)
	}

	si.index[id] = rid
	return nil
}

func (si *SimpleIndex) GetAllRecords() map[int]RecordID {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	result := make(map[int]RecordID)
	for k, v := range si.index {
		result[k] = v
	}
	return result
}
