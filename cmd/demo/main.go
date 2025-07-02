package main

import (
	"fmt"
	"log"
	"os"
	"storage-layer/pkg/layer"
	"storage-layer/pkg/record"
)

func main() {
	storageDir := "./storage_test"
	os.RemoveAll(storageDir)

	storage := layer.NewFileStorageLayer()

	if err := storage.Open(storageDir); err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	schema := record.Schema{
		Columns: []record.Column{
			{Name: "id", Type: record.TypeInt, Nullable: false},
			{Name: "name", Type: record.TypeString, Length: 50, Nullable: false},
			{Name: "age", Type: record.TypeInt, Nullable: true},
		},
	}

	if err := storage.CreateTable("users", schema); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("✅ Table 'users' created successfully")

	records := [][]interface{}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", nil},
		{4, "Diana", 28},
	}

	var recordIDs []int
	for _, recordValues := range records {
		serialized, err := record.Serialize(schema, recordValues)
		if err != nil {
			log.Fatalf("Failed to serialize record: %v", err)
		}

		id, err := storage.Insert("users", serialized)
		if err != nil {
			log.Fatalf("Failed to insert record: %v", err)
		}

		recordIDs = append(recordIDs, id)
		fmt.Printf("✅ Inserted record with ID: %d\n", id)
	}

	fmt.Println("\n📖 Reading records:")
	for _, id := range recordIDs {
		data, err := storage.Get("users", id)
		if err != nil {
			log.Printf("Failed to get record %d: %v", id, err)
			continue
		}

		values, err := record.Deserialize(schema, data)
		if err != nil {
			log.Printf("Failed to deserialize record %d: %v", id, err)
			continue
		}

		fmt.Printf("Record %d: %v\n", id, values)
	}

	fmt.Println("\n✏️  Updating record:")
	updateValues := []interface{}{2, "Bob", 31}
	updatedData, err := record.Serialize(schema, updateValues)
	if err != nil {
		log.Fatalf("Failed to serialize updated record: %v", err)
	}

	if err := storage.Update("users", recordIDs[1], updatedData); err != nil {
		log.Fatalf("Failed to update record: %v", err)
	}

	data, err := storage.Get("users", recordIDs[1])
	if err != nil {
		log.Fatalf("Failed to get updated record: %v", err)
	}

	values, err := record.Deserialize(schema, data)
	if err != nil {
		log.Fatalf("Failed to deserialize updated record: %v", err)
	}

	fmt.Printf("Updated record %d: %v\n", recordIDs[1], values)

	fmt.Println("\n🔍 Scanning all records:")
	allRecords, err := storage.Scan("users", nil)
	if err != nil {
		log.Fatalf("Failed to scan records: %v", err)
	}

	for i, recordData := range allRecords {
		values, err := record.Deserialize(schema, recordData)
		if err != nil {
			log.Printf("Failed to deserialize scanned record %d: %v", i, err)
			continue
		}
		fmt.Printf("Scanned record %d: %v\n", i, values)
	}

	fmt.Println("\n🗑️  Deleting record:")
	if err := storage.DeleteRecord("users", recordIDs[2]); err != nil {
		log.Fatalf("Failed to delete record: %v", err)
	}
	fmt.Printf("Deleted record %d\n", recordIDs[2])

	_, err = storage.Get("users", recordIDs[2])
	if err != nil {
		fmt.Printf("✅ Confirmed record %d is deleted: %v\n", recordIDs[2], err)
	}

	if err := storage.Flush(); err != nil {
		log.Fatalf("Failed to flush: %v", err)
	}

	fmt.Println("\n💾 Storage flushed successfully - data persisted to disk")

	fmt.Println("\n🔄 Testing persistence - reopening storage...")
	storage.Close()

	newStorage := layer.NewFileStorageLayer()
	if err := newStorage.Open(storageDir); err != nil {
		log.Fatalf("Failed to reopen storage: %v", err)
	}
	defer newStorage.Close()

	fmt.Println("📖 Scanning records after restart:")
	allRecords, err = newStorage.Scan("users", nil)
	if err != nil {
		log.Fatalf("Failed to scan records after restart: %v", err)
	}

	for i, recordData := range allRecords {
		values, err := record.Deserialize(schema, recordData)
		if err != nil {
			log.Printf("Failed to deserialize record %d: %v", i, err)
			continue
		}
		fmt.Printf("Persisted record %d: %v\n", i, values)
	}

	fmt.Println("\nMVP Storage Layer Demo Completed Successfully!")
}
