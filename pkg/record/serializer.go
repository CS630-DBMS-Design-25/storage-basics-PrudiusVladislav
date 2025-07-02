package record

import (
	"encoding/binary"
	"fmt"
	"math"
)

type ColumnType string

const (
	TypeInt    ColumnType = "INT"
	TypeFloat  ColumnType = "FLOAT"
	TypeString ColumnType = "STRING"
)

type Column struct {
	Name     string
	Type     ColumnType
	Length   int // Max length for strings
	Nullable bool
}

type Schema struct {
	Columns []Column
}

// Record serialization format:
// [null_bitmap (1 byte per 8 columns)] [field1] [field2] ...
// For strings: [length (2 bytes)] [data]

func Serialize(schema Schema, values []interface{}) ([]byte, error) {
	if len(values) != len(schema.Columns) {
		return nil, fmt.Errorf("value count mismatch: expected %d, got %d", len(schema.Columns), len(values))
	}

	nullBitmapSize := (len(schema.Columns) + 7) / 8
	result := make([]byte, nullBitmapSize)

	for i, col := range schema.Columns {
		value := values[i]

		if value == nil {
			if !col.Nullable {
				return nil, fmt.Errorf("column %s cannot be null", col.Name)
			}

			byteIdx := i / 8
			bitIdx := i % 8
			result[byteIdx] |= (1 << bitIdx)
			continue
		}

		fieldData, err := serializeField(col, value)
		if err != nil {
			return nil, fmt.Errorf("error serializing field %s: %v", col.Name, err)
		}

		result = append(result, fieldData...)
	}

	return result, nil
}

func Deserialize(schema Schema, data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	nullBitmapSize := (len(schema.Columns) + 7) / 8
	if len(data) < nullBitmapSize {
		return nil, fmt.Errorf("insufficient data for null bitmap")
	}

	values := make([]interface{}, len(schema.Columns))
	offset := nullBitmapSize

	for i, col := range schema.Columns {

		byteIdx := i / 8
		bitIdx := i % 8
		isNull := (data[byteIdx] & (1 << bitIdx)) != 0

		if isNull {
			values[i] = nil
			continue
		}

		value, bytesRead, err := deserializeField(col, data[offset:])
		if err != nil {
			return nil, fmt.Errorf("error deserializing field %s: %v", col.Name, err)
		}

		values[i] = value
		offset += bytesRead
	}

	return values, nil
}

func serializeField(col Column, value interface{}) ([]byte, error) {
	switch col.Type {
	case TypeInt:
		intVal, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("expected int, got %T", value)
		}
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(intVal))
		return data, nil

	case TypeFloat:
		floatVal, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64, got %T", value)
		}
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(floatVal))
		return data, nil

	case TypeString:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		if len(strVal) > col.Length {
			return nil, fmt.Errorf("string too long: max %d, got %d", col.Length, len(strVal))
		}

		data := make([]byte, 2+len(strVal))
		binary.LittleEndian.PutUint16(data[0:2], uint16(len(strVal)))
		copy(data[2:], []byte(strVal))
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", col.Type)
	}
}

func deserializeField(col Column, data []byte) (interface{}, int, error) {
	switch col.Type {
	case TypeInt:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for int")
		}
		value := int(binary.LittleEndian.Uint32(data[0:4]))
		return value, 4, nil

	case TypeFloat:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for float")
		}
		bits := binary.LittleEndian.Uint64(data[0:8])
		value := math.Float64frombits(bits)
		return value, 8, nil

	case TypeString:
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for string length")
		}
		length := binary.LittleEndian.Uint16(data[0:2])
		if len(data) < 2+int(length) {
			return nil, 0, fmt.Errorf("insufficient data for string")
		}
		value := string(data[2 : 2+length])
		return value, 2 + int(length), nil

	default:
		return nil, 0, fmt.Errorf("unsupported column type: %s", col.Type)
	}
}
