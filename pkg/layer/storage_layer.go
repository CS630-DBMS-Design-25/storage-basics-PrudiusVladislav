package layer

import (
	"fmt"
	"storage-layer/pkg/bptree"
	"storage-layer/pkg/catalog"
	"storage-layer/pkg/disk"
	"storage-layer/pkg/page"
	"storage-layer/pkg/record"
	"sync"
)

type FileStorageLayer struct {
	basePath    string
	diskManager *disk.DiskManager
	catalog     *catalog.CatalogManager
	pageCache   map[string]map[int32]*page.Page
	indexes     map[string]*bptree.SimpleIndex
	isOpen      bool
	mutex       sync.RWMutex
}

func NewFileStorageLayer() *FileStorageLayer {
	return &FileStorageLayer{
		pageCache: make(map[string]map[int32]*page.Page),
		indexes:   make(map[string]*bptree.SimpleIndex),
		isOpen:    false,
	}
}

func (fsl *FileStorageLayer) Open(path string) error {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if fsl.isOpen {
		return fmt.Errorf("storage layer is already open")
	}

	fsl.basePath = path
	fsl.diskManager = disk.NewDiskManager(path)
	fsl.catalog = catalog.NewCatalogManager(path)

	if err := fsl.diskManager.Open(); err != nil {
		return fmt.Errorf("failed to open disk manager: %v", err)
	}

	if err := fsl.catalog.Load(); err != nil {
		return fmt.Errorf("failed to load catalog: %v", err)
	}

	for _, tableName := range fsl.catalog.ListTables() {
		index := bptree.NewSimpleIndex(tableName, path)
		if err := index.Load(); err != nil {
			return fmt.Errorf("failed to load index for table %s: %v", tableName, err)
		}
		fsl.indexes[tableName] = index
		fsl.pageCache[tableName] = make(map[int32]*page.Page)
	}

	fsl.isOpen = true
	return nil
}

func (fsl *FileStorageLayer) Close() error {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if !fsl.isOpen {
		return nil
	}

	if err := fsl.Flush(); err != nil {
		return fmt.Errorf("failed to flush during close: %v", err)
	}

	if err := fsl.diskManager.Close(); err != nil {
		return fmt.Errorf("failed to close disk manager: %v", err)
	}

	fsl.isOpen = false
	return nil
}

func (fsl *FileStorageLayer) CreateTable(tableName string, schema record.Schema) error {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if !fsl.isOpen {
		return fmt.Errorf("storage layer is not open")
	}

	if err := fsl.catalog.CreateTable(tableName, schema); err != nil {
		return err
	}

	index := bptree.NewSimpleIndex(tableName, fsl.basePath)
	fsl.indexes[tableName] = index
	fsl.pageCache[tableName] = make(map[int32]*page.Page)

	return nil
}

func (fsl *FileStorageLayer) Insert(tableName string, recordData []byte) (int, error) {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if !fsl.isOpen {
		return -1, fmt.Errorf("storage layer is not open")
	}

	if !fsl.catalog.TableExists(tableName) {
		return -1, fmt.Errorf("table %s does not exist", tableName)
	}

	pageID, slotID, err := fsl.insertRecord(tableName, recordData)
	if err != nil {
		return -1, err
	}

	rid := bptree.RecordID{
		PageID: pageID,
		SlotID: slotID,
	}

	recordID, err := fsl.indexes[tableName].Insert(rid)
	if err != nil {
		return -1, fmt.Errorf("failed to insert into index: %v", err)
	}

	return recordID, nil
}

func (fsl *FileStorageLayer) Get(tableName string, recordID int) ([]byte, error) {
	fsl.mutex.RLock()
	defer fsl.mutex.RUnlock()

	if !fsl.isOpen {
		return nil, fmt.Errorf("storage layer is not open")
	}

	rid, exists := fsl.indexes[tableName].Search(recordID)
	if !exists {
		return nil, fmt.Errorf("record %d not found", recordID)
	}

	page, err := fsl.getPage(tableName, rid.PageID)
	if err != nil {
		return nil, err
	}

	return page.GetRecord(rid.SlotID)
}

func (fsl *FileStorageLayer) Update(tableName string, recordID int, updatedRecord []byte) error {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if !fsl.isOpen {
		return fmt.Errorf("storage layer is not open")
	}

	rid, exists := fsl.indexes[tableName].Search(recordID)
	if !exists {
		return fmt.Errorf("record %d not found", recordID)
	}

	page, err := fsl.getPage(tableName, rid.PageID)
	if err != nil {
		return err
	}

	return page.UpdateRecord(rid.SlotID, updatedRecord)
}

func (fsl *FileStorageLayer) DeleteRecord(tableName string, recordID int) error {
	fsl.mutex.Lock()
	defer fsl.mutex.Unlock()

	if !fsl.isOpen {
		return fmt.Errorf("storage layer is not open")
	}

	rid, exists := fsl.indexes[tableName].Search(recordID)
	if !exists {
		return fmt.Errorf("record %d not found", recordID)
	}

	page, err := fsl.getPage(tableName, rid.PageID)
	if err != nil {
		return err
	}

	if err := page.DeleteRecord(rid.SlotID); err != nil {
		return err
	}

	return fsl.indexes[tableName].Delete(recordID)
}

func (fsl *FileStorageLayer) Scan(tableName string, filter func([]byte) bool) ([][]byte, error) {
	fsl.mutex.RLock()
	defer fsl.mutex.RUnlock()

	if !fsl.isOpen {
		return nil, fmt.Errorf("storage layer is not open")
	}

	if !fsl.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	var results [][]byte

	allRecords := fsl.indexes[tableName].GetAllRecords()

	for _, rid := range allRecords {

		page, err := fsl.getPage(tableName, rid.PageID)
		if err != nil {
			continue
		}

		recordData, err := page.GetRecord(rid.SlotID)
		if err != nil {
			continue
		}

		if filter == nil || filter(recordData) {
			results = append(results, recordData)
		}
	}

	return results, nil
}

func (fsl *FileStorageLayer) Flush() error {
	if !fsl.isOpen {
		return fmt.Errorf("storage layer is not open")
	}

	if err := fsl.catalog.Save(); err != nil {
		return fmt.Errorf("failed to save catalog: %v", err)
	}

	for _, index := range fsl.indexes {
		if err := index.Save(); err != nil {
			return fmt.Errorf("failed to save index: %v", err)
		}
	}

	for tableName, pages := range fsl.pageCache {
		for pageID, page := range pages {
			if page.IsDirty() {
				if err := fsl.diskManager.WritePage(tableName, pageID, page.GetData()); err != nil {
					return fmt.Errorf("failed to write page %d for table %s: %v", pageID, tableName, err)
				}
				page.SetClean()
			}
		}
	}

	return nil
}

func (fsl *FileStorageLayer) getPage(tableName string, pageID int32) (*page.Page, error) {

	if pages, exists := fsl.pageCache[tableName]; exists {
		if page, exists := pages[pageID]; exists {
			return page, nil
		}
	}

	data, err := fsl.diskManager.ReadPage(tableName, pageID)
	if err != nil {
		return nil, err
	}

	pg := page.LoadPage(pageID, data)
	if pg == nil {
		return nil, fmt.Errorf("failed to load page %d", pageID)
	}

	if fsl.pageCache[tableName] == nil {
		fsl.pageCache[tableName] = make(map[int32]*page.Page)
	}
	fsl.pageCache[tableName][pageID] = pg

	return pg, nil
}

func (fsl *FileStorageLayer) insertRecord(tableName string, recordData []byte) (int32, int, error) {

	pageCount := fsl.diskManager.GetPageCount(tableName)

	for pageID := int32(0); pageID < pageCount; pageID++ {
		page, err := fsl.getPage(tableName, pageID)
		if err != nil {
			continue
		}

		slotID, err := page.InsertRecord(recordData)
		if err == nil {
			return pageID, slotID, nil
		}
	}

	newPageID, err := fsl.diskManager.AllocatePage(tableName)
	if err != nil {
		return -1, -1, err
	}

	newPage := page.NewPage(newPageID)
	slotID, err := newPage.InsertRecord(recordData)
	if err != nil {
		return -1, -1, err
	}

	if fsl.pageCache[tableName] == nil {
		fsl.pageCache[tableName] = make(map[int32]*page.Page)
	}
	fsl.pageCache[tableName][newPageID] = newPage

	return newPageID, slotID, nil
}
