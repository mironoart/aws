package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type FileWriter struct {
	filePath string
	ch       chan any
}

func NewFileWriter(filePath string) *FileWriter {
	return &FileWriter{
		filePath: filePath,
		ch:       make(chan any),
	}
}

func (fw *FileWriter) Start() error {
	fw.ch = make(chan any)
	_ = os.Remove(fw.filePath)

	f, err := os.OpenFile(fw.filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return err
	}
	defer f.Close()

	if _, err := f.Write([]byte("[\n")); err != nil {
		fmt.Printf("Error writing to file: %v\n", err)
		return err
	}
	fmt.Println("Created file")
	go func() {

		f, err := os.OpenFile(fw.filePath, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Error creating file: %v\n", err)
			return
		}
		defer f.Close()

		for data := range fw.ch {
			bytes, err := json.MarshalIndent(data, "  ", "  ")
			if err != nil {
				fmt.Printf("Error marshaling JSON: %v\n", err)
				continue
			}

			if _, err := f.Write(bytes); err != nil {
				fmt.Printf("Error writing JSON: %v\n", err)
			}

			if _, err := f.Write([]byte(",\n")); err != nil {
				fmt.Printf("Error writing comma: %v\n", err)
			}

		}

	}()
	return nil
}

func (fw *FileWriter) Append(data any) {
	fw.ch <- data
}

func (fw *FileWriter) Close() error {
	// Read the file content
	data, err := os.ReadFile(fw.filePath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return err
	}

	s := string(data)

	// Find the last comma and closing bracket
	lastComma := strings.LastIndex(s, ",")
	lastBracket := strings.LastIndex(s, "]")

	// Remove trailing comma if it's before the closing bracket
	if lastComma != -1 && (lastBracket == -1 || lastComma < lastBracket) {
		s = s[:lastComma] + s[lastComma+1:]
	}

	// Ensure file ends with a closing bracket
	if !strings.HasSuffix(s, "]") {
		s = strings.TrimSpace(s) + "]"
	}

	// Rewrite the file
	err = os.WriteFile(fw.filePath, []byte(s), 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		return err
	}

	// Close the writer channel (if applicable)
	close(fw.ch)
	fmt.Println("Closed file and removed last comma successfully")

	return nil
}
