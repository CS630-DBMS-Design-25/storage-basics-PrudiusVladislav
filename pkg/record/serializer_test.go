package record

import (
	"reflect"
	"testing"
)

func TestRecordSerialization(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt, Nullable: false},
			{Name: "name", Type: TypeString, Length: 50, Nullable: false},
			{Name: "age", Type: TypeInt, Nullable: true},
			{Name: "score", Type: TypeFloat, Nullable: false},
		},
	}

	// Test normal record
	values := []interface{}{1, "Alice", 25, 95.5}

	serialized, err := Serialize(schema, values)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	deserialized, err := Deserialize(schema, serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if !reflect.DeepEqual(values, deserialized) {
		t.Errorf("Values don't match: expected %v, got %v", values, deserialized)
	}

	// Test record with null
	valuesWithNull := []interface{}{2, "Bob", nil, 87.3}

	serialized, err = Serialize(schema, valuesWithNull)
	if err != nil {
		t.Fatalf("Failed to serialize with null: %v", err)
	}

	deserialized, err = Deserialize(schema, serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize with null: %v", err)
	}

	if !reflect.DeepEqual(valuesWithNull, deserialized) {
		t.Errorf("Values with null don't match: expected %v, got %v", valuesWithNull, deserialized)
	}
}
