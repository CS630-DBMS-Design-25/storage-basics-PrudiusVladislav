package page

import (
	"testing"
)

func TestPageOperations(t *testing.T) {
	// Create a new page
	page := NewPage(0)

	// Debug: Check initial page state
	initialHeader := page.readHeader()
	t.Logf("Initial header: PageID=%d, SlotCount=%d, FreeStart=%d, FreeEnd=%d",
		initialHeader.PageID, initialHeader.SlotCount, initialHeader.FreeStart, initialHeader.FreeEnd)

	// Test inserting records
	record1 := []byte("Hello World")
	slot1, err := page.InsertRecord(record1)
	if err != nil {
		t.Fatalf("Failed to insert record 1: %v", err)
	}

	record2 := []byte("Goodbye World")
	slot2, err := page.InsertRecord(record2)
	if err != nil {
		t.Fatalf("Failed to insert record 2: %v", err)
	}
	t.Logf("Inserted record 1 at slot %d", slot1)
	t.Logf("Inserted record 2 at slot %d", slot2)

	// Debug: Check what was actually written to slots
	t.Logf("Raw slot data for slot 0:")
	slotOffset := PageHeaderSize + 0*SlotEntrySize
	rawOffset := page.Data[slotOffset : slotOffset+2]
	rawSize := page.Data[slotOffset+2 : slotOffset+4]
	t.Logf("Raw offset bytes: %v", rawOffset)
	t.Logf("Raw size bytes: %v", rawSize)

	// Test reading records
	retrieved1, err := page.GetRecord(slot1)
	if err != nil {
		t.Fatalf("Failed to get record 1: %v", err)
	}

	retrieved2, err := page.GetRecord(slot2)
	if err != nil {
		t.Fatalf("Failed to get record 2: %v", err)
	}

	if string(retrieved1) != string(record1) {
		t.Errorf("Record 1 mismatch: expected %s, got %s", string(record1), string(retrieved1))
	}

	if string(retrieved2) != string(record2) {
		t.Errorf("Record 2 mismatch: expected %s, got %s", string(record2), string(retrieved2))
	}

	// Test page persistence
	pageData := page.GetData()

	// Load page from data
	loadedPage := LoadPage(0, pageData)
	if loadedPage == nil {
		t.Fatal("Failed to load page from data")
	}

	// Test reading from loaded page
	loadedRecord1, err := loadedPage.GetRecord(slot1)
	if err != nil {
		t.Fatalf("Failed to get record 1 from loaded page: %v", err)
	}

	if string(loadedRecord1) != string(record1) {
		t.Errorf("Loaded record 1 mismatch: expected %s, got %s", string(record1), string(loadedRecord1))
	}

	// Debug: Print header and slot information
	header := page.readHeader()
	t.Logf("Page header: PageID=%d, SlotCount=%d, FreeStart=%d, FreeEnd=%d",
		header.PageID, header.SlotCount, header.FreeStart, header.FreeEnd)

	for i := 0; i < int(header.SlotCount); i++ {
		slot := page.readSlot(i)
		t.Logf("Slot %d: Offset=%d, Size=%d", i, slot.Offset, slot.Size)
	}
}
