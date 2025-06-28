package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

var targetCodes = map[string]bool{
	"99283": true,
	"99284": true,
	"99285": true,
	"99291": true,
}

type InNetworkObj map[string]interface{}

// Optimized search for known JSON structure
func findMatchingObjectsOptimized(data interface{}) []InNetworkObj {
	var matches []InNetworkObj
	var processedCount int64

	// Use a closure to track progress
	var search func(interface{}) []InNetworkObj
	search = func(data interface{}) []InNetworkObj {
		processedCount++
		if processedCount%10000 == 0 {
			fmt.Printf("\rSearching... Processed %d objects", processedCount)
		}

		var localMatches []InNetworkObj

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

func findMatchingObjects(data interface{}) []InNetworkObj {
	return findMatchingObjectsOptimized(data)
}

func main() {
	fmt.Println("Starting JSON parser...")

	jsonFile, err := os.Open("billing_code_matches.json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()

	// Get file size for progress tracking
	fileInfo, err := jsonFile.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fileInfo.Size()

	fmt.Printf("Loading JSON file into memory... (File size: %.2f MB)\n", float64(fileSize)/(1024*1024))

	// Create a progress reader
	progressReader := &ProgressReader{
		Reader: jsonFile,
		Total:  fileSize,
		Callback: func(percent float64) {
			fmt.Printf("\rProgress: %.1f%%", percent)
		},
	}

	var data interface{}
	if err := json.NewDecoder(progressReader).Decode(&data); err != nil {
		panic(err)
	}
	fmt.Println("\nJSON file loaded successfully!")

	fmt.Println("Searching for objects with billing codes: 99283, 99284, 99285, 99291")
	records := findMatchingObjects(data)

	fmt.Printf("Found %d matching objects\n", len(records))

	if len(records) == 0 {
		fmt.Println("No matching billing codes found")
		return
	}

	fmt.Println("Writing matching objects to output file")
	outputFile, err := os.Create("billing_code_matches.json")
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(records); err != nil {
		panic(err)
	}

	fmt.Printf("Done! %d matching objects written to billing_code_matches.json\n", len(records))

	ExtractToCSV()
}

// ProgressReader wraps an io.Reader and reports progress
type ProgressReader struct {
	Reader    io.Reader
	Total     int64
	BytesRead int64
	Callback  func(float64)
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	pr.BytesRead += int64(n)

	if pr.Callback != nil {
		percent := float64(pr.BytesRead) / float64(pr.Total) * 100
		pr.Callback(percent)
	}

	return n, err
}
