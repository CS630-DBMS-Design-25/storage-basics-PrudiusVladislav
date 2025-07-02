package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"storage-layer/pkg/record"
	"sync"
)

type CatalogManager struct {
	basePath string
	schemas  map[string]record.Schema
	mutex    sync.RWMutex
}

func NewCatalogManager(basePath string) *CatalogManager {
	return &CatalogManager{
		basePath: basePath,
		schemas:  make(map[string]record.Schema),
	}
}

func (cm *CatalogManager) Load() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	metaPath := filepath.Join(cm.basePath, "tables.meta")

	// If meta file doesn't exist, start with empty catalog
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return fmt.Errorf("failed to read catalog: %v", err)
	}

	var schemas map[string]record.Schema
	if err := json.Unmarshal(data, &schemas); err != nil {
		return fmt.Errorf("failed to unmarshal catalog: %v", err)
	}

	cm.schemas = schemas
	return nil
}

func (cm *CatalogManager) Save() error {
	metaPath := filepath.Join(cm.basePath, "tables.meta")

	data, err := json.MarshalIndent(cm.schemas, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %v", err)
	}

	return os.WriteFile(metaPath, data, 0644)
}

func (cm *CatalogManager) CreateTable(tableName string, schema record.Schema) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if _, exists := cm.schemas[tableName]; exists {
		return fmt.Errorf("table %s already exists", tableName)
	}

	cm.schemas[tableName] = schema
	return cm.Save()
}

func (cm *CatalogManager) GetSchema(tableName string) (record.Schema, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	schema, exists := cm.schemas[tableName]
	if !exists {
		return record.Schema{}, fmt.Errorf("table %s does not exist", tableName)
	}

	return schema, nil
}

func (cm *CatalogManager) TableExists(tableName string) bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	_, exists := cm.schemas[tableName]
	return exists
}

func (cm *CatalogManager) ListTables() []string {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	tables := make([]string, 0, len(cm.schemas))
	for table := range cm.schemas {
		tables = append(tables, table)
	}
	return tables
}
