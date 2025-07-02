package page

import (
	"encoding/binary"
	"fmt"
)

const PageSize = 4096

// Page layout:
// [PageHeader: 18 bytes] [SlotDirectory] [Free Space] [Records]
type PageHeader struct {
	PageID     int32 // 4 bytes
	SlotCount  int16 // 2 bytes
	FreeStart  int16 // 2 bytes - offset where free space starts
	FreeEnd    int16 // 2 bytes - offset where free space ends
	NextPageID int32 // 4 bytes
	PrevPageID int32 // 4 bytes
}

type SlotEntry struct {
	Offset int16 // 2 bytes - offset to record data
	Size   int16 // 2 bytes - size of record
}

const (
	PageHeaderSize = 18 // Fixed to accommodate full header
	SlotEntrySize  = 4
)

type Page struct {
	PageID int32
	Data   [PageSize]byte
	dirty  bool
}

func NewPage(pageID int32) *Page {
	page := &Page{
		PageID: pageID,
		dirty:  true,
	}

	header := PageHeader{
		PageID:     pageID,
		SlotCount:  0,
		FreeStart:  PageHeaderSize,
		FreeEnd:    PageSize,
		NextPageID: -1,
		PrevPageID: -1,
	}

	page.writeHeader(header)
	return page
}

func LoadPage(pageID int32, data []byte) *Page {
	if len(data) != PageSize {
		return nil
	}

	page := &Page{
		PageID: pageID,
		dirty:  false,
	}
	copy(page.Data[:], data)
	return page
}

func (p *Page) readHeader() PageHeader {
	var header PageHeader
	header.PageID = int32(binary.LittleEndian.Uint32(p.Data[0:4]))
	header.SlotCount = int16(binary.LittleEndian.Uint16(p.Data[4:6]))
	header.FreeStart = int16(binary.LittleEndian.Uint16(p.Data[6:8]))
	header.FreeEnd = int16(binary.LittleEndian.Uint16(p.Data[8:10]))
	header.NextPageID = int32(binary.LittleEndian.Uint32(p.Data[10:14]))
	header.PrevPageID = int32(binary.LittleEndian.Uint32(p.Data[14:18]))
	return header
}

func (p *Page) writeHeader(header PageHeader) {
	binary.LittleEndian.PutUint32(p.Data[0:4], uint32(header.PageID))
	binary.LittleEndian.PutUint16(p.Data[4:6], uint16(header.SlotCount))
	binary.LittleEndian.PutUint16(p.Data[6:8], uint16(header.FreeStart))
	binary.LittleEndian.PutUint16(p.Data[8:10], uint16(header.FreeEnd))
	binary.LittleEndian.PutUint32(p.Data[10:14], uint32(header.NextPageID))
	binary.LittleEndian.PutUint32(p.Data[14:18], uint32(header.PrevPageID))
	p.dirty = true
}

func (p *Page) readSlot(slotID int) SlotEntry {
	offset := PageHeaderSize + slotID*SlotEntrySize
	var slot SlotEntry
	slot.Offset = int16(binary.LittleEndian.Uint16(p.Data[offset : offset+2]))
	slot.Size = int16(binary.LittleEndian.Uint16(p.Data[offset+2 : offset+4]))
	return slot
}

func (p *Page) writeSlot(slotID int, slot SlotEntry) {
	offset := PageHeaderSize + slotID*SlotEntrySize
	binary.LittleEndian.PutUint16(p.Data[offset:offset+2], uint16(slot.Offset))
	binary.LittleEndian.PutUint16(p.Data[offset+2:offset+4], uint16(slot.Size))
	p.dirty = true
}

func (p *Page) InsertRecord(record []byte) (int, error) {
	header := p.readHeader()
	recordSize := len(record)

	freeSpace := int(header.FreeEnd) - int(header.FreeStart) - SlotEntrySize
	if freeSpace < recordSize {
		return -1, fmt.Errorf("not enough space in page")
	}

	slotID := -1
	for i := 0; i < int(header.SlotCount); i++ {
		slot := p.readSlot(i)
		if slot.Size == 0 {
			slotID = i
			break
		}
	}

	if slotID == -1 {
		slotID = int(header.SlotCount)
		header.SlotCount++
		header.FreeStart += SlotEntrySize
	}

	recordOffset := int(header.FreeEnd) - recordSize
	copy(p.Data[recordOffset:recordOffset+recordSize], record)

	slot := SlotEntry{
		Offset: int16(recordOffset),
		Size:   int16(recordSize),
	}
	p.writeSlot(slotID, slot)

	header.FreeEnd = int16(recordOffset)
	p.writeHeader(header)

	return slotID, nil
}

func (p *Page) GetRecord(slotID int) ([]byte, error) {
	header := p.readHeader()
	if slotID >= int(header.SlotCount) {
		return nil, fmt.Errorf("slot %d does not exist", slotID)
	}

	slot := p.readSlot(slotID)
	if slot.Size == 0 {
		return nil, fmt.Errorf("slot %d is empty", slotID)
	}

	if slot.Offset < 0 || slot.Size < 0 {
		return nil, fmt.Errorf("invalid slot data: offset=%d, size=%d", slot.Offset, slot.Size)
	}

	endOffset := int(slot.Offset) + int(slot.Size)
	if endOffset > PageSize {
		return nil, fmt.Errorf("slot data exceeds page bounds: offset=%d, size=%d", slot.Offset, slot.Size)
	}

	record := make([]byte, slot.Size)
	copy(record, p.Data[slot.Offset:slot.Offset+slot.Size])
	return record, nil
}

func (p *Page) UpdateRecord(slotID int, newRecord []byte) error {
	header := p.readHeader()
	if slotID >= int(header.SlotCount) {
		return fmt.Errorf("slot %d does not exist", slotID)
	}

	slot := p.readSlot(slotID)
	if slot.Size == 0 {
		return fmt.Errorf("slot %d is empty", slotID)
	}

	if len(newRecord) != int(slot.Size) {
		return fmt.Errorf("record size mismatch: expected %d, got %d", slot.Size, len(newRecord))
	}

	copy(p.Data[slot.Offset:slot.Offset+slot.Size], newRecord)
	p.dirty = true
	return nil
}

func (p *Page) DeleteRecord(slotID int) error {
	header := p.readHeader()
	if slotID >= int(header.SlotCount) {
		return fmt.Errorf("slot %d does not exist", slotID)
	}

	slot := p.readSlot(slotID)
	if slot.Size == 0 {
		return fmt.Errorf("slot %d is already empty", slotID)
	}

	emptySlot := SlotEntry{Offset: 0, Size: 0}
	p.writeSlot(slotID, emptySlot)

	return nil
}

func (p *Page) IsDirty() bool {
	return p.dirty
}

func (p *Page) SetClean() {
	p.dirty = false
}

func (p *Page) GetData() []byte {
	return p.Data[:]
}
