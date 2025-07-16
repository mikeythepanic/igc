package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"unicode"
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

// findMatchingObjectsRecursive recursively searches for objects with a matching billing_code.
// It's used as a fallback for JSON files that are not a simple array of records.
func findMatchingObjectsRecursive(data interface{}) []map[string]interface{} {
	var matches []map[string]interface{}

	var search func(d interface{})
	search = func(d interface{}) {
		switch v := d.(type) {
		case map[string]interface{}:
			// Check if this object itself is a match.
			if code, ok := v["billing_code"].(string); ok && targetCodes[code] {
				matches = append(matches, v)
			}
			// Recursively search all values in the map.
			for _, val := range v {
				search(val)
			}
		case []interface{}:
			// Recursively search all elements in the slice.
			for _, item := range v {
				search(item)
			}
		}
	}

	search(data)
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

// processedFilesLog is the file that tracks processed files
const processedFilesLog = "processed_files.json"

// loadProcessedFiles loads the set of already processed files from the log
func loadProcessedFiles() (map[string]bool, error) {
	files := make(map[string]bool)
	file, err := os.Open(processedFilesLog)
	if err != nil {
		if os.IsNotExist(err) {
			return files, nil // No log yet
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
		return files, nil // Empty log, treat as no files processed
	}

	var fileList []string
	if err := json.NewDecoder(file).Decode(&fileList); err != nil {
		// If the file contains an empty array "[]", it's valid but we should handle it
		if err == io.EOF {
			return files, nil
		}
		return nil, err
	}
	for _, f := range fileList {
		files[f] = true
	}
	return files, nil
}

// saveProcessedFiles saves the set of processed files to the log
func saveProcessedFiles(files map[string]bool) error {
	fileList := make([]string, 0, len(files))
	for f := range files {
		fileList = append(fileList, f)
	}
	file, err := os.Create(processedFilesLog)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(fileList)
}

// processFileAndWriteMatches streams a JSON file, finds matches, and writes them directly to the output writer.
// This avoids loading the entire file into memory and handles JSON files structured as an array of objects.
func processFileAndWriteMatches(filePath string, writer io.Writer) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Use a buffered reader to allow peeking at the first characters without consuming them.
	br := bufio.NewReader(file)
	var firstChar byte

	// Loop to skip any leading whitespace and find the first actual character.
	for {
		b, err := br.ReadByte()
		if err != nil {
			if err == io.EOF {
				return 0, nil // File is empty, which is not an error.
			}
			return 0, err
		}
		if !unicode.IsSpace(rune(b)) {
			firstChar = b
			break
		}
	}

	// Put the character back into the reader stream so the JSON decoder can see it.
	if err := br.UnreadByte(); err != nil {
		return 0, err
	}

	decoder := json.NewDecoder(br)
	encoder := json.NewEncoder(writer)
	count := 0

	// Handle file based on whether it's a JSON array or object.
	if firstChar == '[' {
		// It's an array. Consume the opening '['.
		if _, err := decoder.Token(); err != nil {
			return 0, fmt.Errorf("failed to read opening `[`: %w", err)
		}

		// Loop through the array elements as long as there are more.
		for decoder.More() {
			var record map[string]interface{}
			if err := decoder.Decode(&record); err != nil {
				fmt.Printf("\nWarning: could not decode an object in %s: %v. Skipping object.", filepath.Base(filePath), err)
				continue
			}

			if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
				if err := encoder.Encode(record); err != nil {
					return count, fmt.Errorf("failed to write matched record to output: %w", err)
				}
				count++
			}
		}
	} else if firstChar == '{' {
		// It's a single root object. Decode it all into memory.
		var data map[string]interface{}
		if err := decoder.Decode(&data); err != nil {
			return 0, fmt.Errorf("failed to decode root object: %w", err)
		}

		// Recursively search the object for matches.
		matches := findMatchingObjectsRecursive(data)
		for _, match := range matches {
			if err := encoder.Encode(match); err != nil {
				return count, fmt.Errorf("failed to write matched record from object: %w", err)
			}
			count++
		}
	} else {
		return 0, fmt.Errorf("file does not appear to be valid JSON (starts with %c)", firstChar)
	}

	return count, nil
}

// A result struct to pass information back from workers.
type result struct {
	fileName     string
	recordsFound int
	err          error
}

// worker is the function that will be run concurrently.
// It reads file paths from the jobs channel, processes them, and sends the result to the results channel.
func worker(id int, jobs <-chan string, results chan<- result, writer io.Writer, writerMutex *sync.Mutex) {
	for filePath := range jobs {
		// Each worker locks the writer before processing a file to ensure that
		// all writes from a single file are contiguous and not interleaved with other workers.
		writerMutex.Lock()
		recordsFound, err := processFileAndWriteMatches(filePath, writer)
		writerMutex.Unlock()

		results <- result{
			fileName:     filepath.Base(filePath),
			recordsFound: recordsFound,
			err:          err,
		}
	}
}

func main() {
	fmt.Println("Starting JSON parser with directory processing...")

	// Directory to process and the new output file using JSON Lines format
	dirPath := "../decompress/output"
	outputFile := "matches.jsonl" // Using .jsonl for streaming

	// Read all files in the directory
	allFiles, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	// Load processed files log to filter out files that have already been processed.
	processedFiles, err := loadProcessedFiles()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d previously processed files from %s\n", len(processedFiles), processedFilesLog)

	var filesToProcess []string
	for _, file := range allFiles {
		fileName := file.Name()
		if !file.IsDir() && strings.HasSuffix(strings.ToLower(fileName), ".json") {
			if !processedFiles[fileName] {
				filesToProcess = append(filesToProcess, filepath.Join(dirPath, fileName))
			}
		}
	}

	if len(filesToProcess) == 0 {
		fmt.Println("No new files to process.")
		return
	}

	fmt.Printf("Found %d new JSON files to process\n", len(filesToProcess))

	// --- Concurrency Setup ---
	// A conservative number of workers: half of the available CPUs, but at least 1.
	numWorkers := runtime.NumCPU() / 2
	if numWorkers < 1 {
		numWorkers = 1
	}
	fmt.Printf("Using %d worker(s) to process files...\n", numWorkers)

	jobs := make(chan string, len(filesToProcess))
	results := make(chan result, len(filesToProcess))
	var writerMutex = &sync.Mutex{}

	// Open the output file in append mode. It will be created if it doesn't exist.
	out, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	// Start workers.
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, results, out, writerMutex)
	}

	// Send jobs to the workers.
	for _, filePath := range filesToProcess {
		jobs <- filePath
	}
	close(jobs)

	// --- Collect Results ---
	totalNewRecords := 0
	filesProcessed := 0
	for i := 0; i < len(filesToProcess); i++ {
		res := <-results
		filesProcessed++
		if res.err != nil {
			fmt.Printf("\n[%d/%d] Error processing %s: %v", filesProcessed, len(filesToProcess), res.fileName, res.err)
		} else {
			if res.recordsFound > 0 {
				fmt.Printf("\n[%d/%d] Processed %s, found %d records.", filesProcessed, len(filesToProcess), res.fileName, res.recordsFound)
				totalNewRecords += res.recordsFound
			}
			// Mark file as processed in memory
			processedFiles[res.fileName] = true
		}
	}
	fmt.Println() // Newline after progress updates.

	// Save the processed files log once at the end
	if err := saveProcessedFiles(processedFiles); err != nil {
		fmt.Printf("\nWarning: could not update processed files log: %v\n", err)
	}

	fmt.Printf("\nProcessing complete!\n")
	fmt.Printf("Total new records added: %d\n", totalNewRecords)
	fmt.Printf("Files processed in this run: %d\n", filesProcessed)
	fmt.Printf("Files skipped (already processed): %d\n", len(allFiles)-len(filesToProcess))

	// The ExtractToCSV function will need to be updated to handle the .jsonl format.
	// For now, it is commented out to prevent errors.
	ExtractToCSV()
}
