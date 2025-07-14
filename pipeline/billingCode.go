package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"jsonformatter"
)

var targetCodes = map[string]bool{
	"99283": true,
	"99284": true,
	"99285": true,
	"99291": true,
}

// Structured search using the known schema
func findMatchingObjectsStructured(data interface{}) []map[string]interface{} {
	var matches []map[string]interface{}
	var processedCount int64

	// Check if data is an array of records
	if records, ok := data.([]interface{}); ok {
		fmt.Printf("Found %d top-level records to process\n", len(records))

		for _, recordInterface := range records {
			processedCount++
			if processedCount%1000 == 0 {
				fmt.Printf("\rSearching... Processed %d records", processedCount)
			}

			// Try to parse as ICD10Record
			if recordMap, ok := recordInterface.(map[string]interface{}); ok {
				if billingCode, exists := recordMap["billing_code"].(string); exists && targetCodes[billingCode] {
					matches = append(matches, recordMap)
				}
			}
		}
	} else {
		// Fallback to recursive search if structure is different
		fmt.Println("Data is not in expected array format, falling back to recursive search")
		matches = findMatchingObjectsRecursive(data)
	}

	fmt.Printf("\nSearch completed. Processed %d total records\n", processedCount)
	return matches
}

// Keep the original recursive search as fallback
func findMatchingObjectsRecursive(data interface{}) []map[string]interface{} {
	var matches []map[string]interface{}
	var processedCount int64

	var search func(interface{}) []map[string]interface{}
	search = func(data interface{}) []map[string]interface{} {
		processedCount++
		if processedCount%10000 == 0 {
			fmt.Printf("\rSearching... Processed %d objects", processedCount)
		}

		var localMatches []map[string]interface{}

		switch v := data.(type) {
		case map[string]interface{}:
			// Check if this object has a matching billing_code
			if code, ok := v["billing_code"].(string); ok && targetCodes[code] {
				localMatches = append(localMatches, v)
			}
			// Recursively search all values in this object
			for _, val := range v {
				subMatches := search(val)
				localMatches = append(localMatches, subMatches...)
			}
		case []interface{}:
			// Recursively search all elements in this array
			for _, item := range v {
				subMatches := search(item)
				localMatches = append(localMatches, subMatches...)
			}
		}

		return localMatches
	}

	matches = search(data)
	fmt.Printf("\nSearch completed. Processed %d total objects\n", processedCount)
	return matches
}

func findMatchingObjects(data interface{}) []map[string]interface{} {
	return findMatchingObjectsStructured(data)
}

// loadExistingData loads existing records from the output file if it exists
func loadExistingData(filename string) ([]map[string]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, return empty slice
			return []map[string]interface{}{}, nil
		}
		return nil, err
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.Size() == 0 {
		// File is empty, return empty slice
		return []map[string]interface{}{}, nil
	}

	var existingRecords []map[string]interface{}
	if err := json.NewDecoder(file).Decode(&existingRecords); err != nil {
		return nil, err
	}

	return existingRecords, nil
}

// appendToFile appends new records to the existing file
func appendToFile(filename string, newRecords []map[string]interface{}) error {
	// Load existing data
	existingRecords, err := loadExistingData(filename)
	if err != nil {
		return err
	}

	// Combine existing and new records
	allRecords := append(existingRecords, newRecords...)

	// Write back to file
	outputFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(allRecords); err != nil {
		return err
	}

	return nil
}

// processJSONFile processes a single JSON file and returns matching records
func processJSONFile(filePath string) ([]map[string]interface{}, error) {
	// Get file size for progress tracking
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	fmt.Printf("Processing %s (%.2f MB)...\n", filepath.Base(filePath), float64(fileSize)/(1024*1024))

	// First, validate that the JSON file is complete
	if !isValidJSONFile(filePath) {
		return nil, fmt.Errorf("file contains incomplete or invalid JSON (likely from partial decompression)")
	}

	// Use jsonformatter to parse the JSON file
	data, err := jsonformatter.ParseAndFormatJSON(filePath)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON file: %v", err)
	}
	fmt.Println("JSON file loaded successfully!")

	fmt.Println("Searching for objects with billing codes: 99283, 99284, 99285, 99291")
	records := findMatchingObjects(data)

	fmt.Printf("Found %d matching objects in %s\n", len(records), filepath.Base(filePath))
	return records, nil
}

// isValidJSONFile checks if a file contains valid, complete JSON
func isValidJSONFile(filename string) bool {
	file, err := os.Open(filename)
	if err != nil {
		return false
	}
	defer file.Close()

	// Try to decode the entire JSON file
	var data interface{}
	decoder := json.NewDecoder(file)

	// This will fail if JSON is incomplete
	err = decoder.Decode(&data)
	if err != nil {
		return false
	}

	// Check if there's extra data after the JSON (shouldn't be for well-formed JSON)
	var extra interface{}
	err = decoder.Decode(&extra)
	if err != nil && err != io.EOF {
		return false
	}

	return true
}

func main() {
	fmt.Println("Starting JSON parser with directory processing...")

	// Directory to process
	dirPath := "../decompress/output"
	outputFile := "matches.json"

	// Read all files in the directory
	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	// Filter for JSON files
	var jsonFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(strings.ToLower(file.Name()), ".json") {
			jsonFiles = append(jsonFiles, filepath.Join(dirPath, file.Name()))
		}
	}

	fmt.Printf("Found %d JSON files to process\n", len(jsonFiles))

	// Load existing data
	existingRecords, err := loadExistingData(outputFile)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d existing records from %s\n", len(existingRecords), outputFile)

	totalNewRecords := 0

	// Process each JSON file
	for i, filePath := range jsonFiles {
		fmt.Printf("\n[%d/%d] ", i+1, len(jsonFiles))

		records, err := processJSONFile(filePath)
		if err != nil {
			fmt.Printf("Error processing %s: %v\n", filepath.Base(filePath), err)
			continue
		}

		if len(records) > 0 {
			// Append new records to the output file
			if err := appendToFile(outputFile, records); err != nil {
				fmt.Printf("Error appending to output file: %v\n", err)
				continue
			}
			totalNewRecords += len(records)
			fmt.Printf("Appended %d records to %s\n", len(records), outputFile)
		}
	}

	// Get final count
	finalRecords, err := loadExistingData(outputFile)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nProcessing complete!\n")
	fmt.Printf("Total new records added: %d\n", totalNewRecords)
	fmt.Printf("Total records in %s: %d\n", outputFile, len(finalRecords))

	ExtractToCSV()
}
