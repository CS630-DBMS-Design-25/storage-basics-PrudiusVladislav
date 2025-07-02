package disk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskManager(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "disk_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dm := NewDiskManager(tempDir)

	if err := dm.Open(); err != nil {
		t.Fatalf("Failed to open disk manager: %v", err)
	}
	defer dm.Close()

	tableName := "test_table"

	pageID, err := dm.AllocatePage(tableName)
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	if pageID != 0 {
		t.Errorf("Expected first page ID to be 0, got %d", pageID)
	}

	// Test WritePage
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	if err := dm.WritePage(tableName, pageID, testData); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Test ReadPage
	readData, err := dm.ReadPage(tableName, pageID)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("Read data doesn't match written data")
	}

	// Test file exists
	expectedFile := filepath.Join(tempDir, tableName+".tbl")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected file %s to exist", expectedFile)
	}
}
