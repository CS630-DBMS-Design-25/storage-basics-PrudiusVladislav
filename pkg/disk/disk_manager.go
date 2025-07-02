package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const PageSize = 4096

type DiskManager struct {
	basePath    string
	files       map[string]*os.File
	pageCounter map[string]int32
	mutex       sync.RWMutex
}

func NewDiskManager(basePath string) *DiskManager {
	return &DiskManager{
		basePath:    basePath,
		files:       make(map[string]*os.File),
		pageCounter: make(map[string]int32),
	}
}

func (dm *DiskManager) Open() error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	// Create base directory if it doesn't exist
	return os.MkdirAll(dm.basePath, 0755)
}

func (dm *DiskManager) Close() error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	for _, file := range dm.files {
		if err := file.Close(); err != nil {
			return err
		}
	}
	dm.files = make(map[string]*os.File)
	return nil
}

func (dm *DiskManager) getFile(tableName string) (*os.File, error) {
	if file, exists := dm.files[tableName]; exists {
		return file, nil
	}

	filePath := filepath.Join(dm.basePath, tableName+".tbl")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	dm.files[tableName] = file

	// Initialize page counter if not exists
	if _, exists := dm.pageCounter[tableName]; !exists {
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		dm.pageCounter[tableName] = int32(stat.Size() / PageSize)
	}

	return file, nil
}

func (dm *DiskManager) ReadPage(tableName string, pageID int32) ([]byte, error) {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	file, err := dm.getFile(tableName)
	if err != nil {
		return nil, err
	}

	data := make([]byte, PageSize)
	offset := int64(pageID) * PageSize

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (dm *DiskManager) WritePage(tableName string, pageID int32, data []byte) error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	if len(data) != PageSize {
		return fmt.Errorf("page data must be exactly %d bytes", PageSize)
	}

	file, err := dm.getFile(tableName)
	if err != nil {
		return err
	}

	offset := int64(pageID) * PageSize
	_, err = file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	return file.Sync()
}

func (dm *DiskManager) AllocatePage(tableName string) (int32, error) {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	_, err := dm.getFile(tableName)
	if err != nil {
		return -1, err
	}

	pageID := dm.pageCounter[tableName]
	dm.pageCounter[tableName]++

	return pageID, nil
}

func (dm *DiskManager) GetPageCount(tableName string) int32 {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	return dm.pageCounter[tableName]
}
