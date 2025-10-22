package storage

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
)

func WriteToCSV(filePath string, data any) error {
	log.Println("Starting to write CSV to", filePath, ".csv")
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %T", data)
	}

	if v.Len() == 0 {
		return fmt.Errorf("empty slice provided")
	}

	elemType := v.Index(0).Type()

	file, err := os.Create(filePath + ".csv")
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// --- HEADER ---
	var headers []string
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		tag := field.Tag.Get("json")
		if tag == "" {
			headers = append(headers, field.Name)
		} else {
			headers = append(headers, tag)
		}
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// --- ROWS ---
	for i := 0; i < v.Len(); i++ {
		rowVal := v.Index(i)
		var record []string
		for j := 0; j < rowVal.NumField(); j++ {
			field := rowVal.Field(j).Interface()
			switch val := field.(type) {
			case string:
				record = append(record, val)
			case *string:
				if val != nil {
					record = append(record, *val)
				} else {
					record = append(record, "")
				}
			case int64:
				record = append(record, strconv.FormatInt(val, 10))
			case float64:
				record = append(record, strconv.FormatFloat(val, 'f', 2, 64))
			case bool:
				record = append(record, strconv.FormatBool(val))
			default:
				record = append(record, fmt.Sprintf("%v", val))
			}
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	fmt.Printf("✅ CSV file written successfully: %s\n", filePath)
	return nil
}

func ReadFile[T any](filePath string) T {
	log.Println("Reading file" + filePath)
	var data T
	file, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filePath, err)
	}
	if err := json.Unmarshal(file, &data); err != nil {
		log.Fatalf("Error unmarshaling JSON from file %s: %v", filePath, err)
	}
	return data
}
func WriteToJSON(filePath string, data any) {
	log.Println("Writing file" + filePath + ".json")
	file, err := os.Create(filePath + ".json")
	if err != nil {
		log.Fatalf("Error creating file %s: %v", filePath, err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(data); err != nil {
		log.Fatalf("Error encoding JSON to file %s: %v", filePath, err)
	}
}
