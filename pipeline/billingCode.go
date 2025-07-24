package main

import (
	"bufio"
	"compress/gzip"
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

// findMatchingObjectsRecursive recursively searches for objects with a matching billing_code.
// Used as a fallback for JSON files that are not a simple array of records.
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

// StreamingGzipProcessor provides streaming processing of gzip files
type StreamingGzipProcessor struct {
	decoder    *json.Decoder
	gzipReader *gzip.Reader
	file       *os.File
}

// NewStreamingGzipProcessor creates a new streaming processor for gzip files
func NewStreamingGzipProcessor(gzipFilePath string) (*StreamingGzipProcessor, error) {
	file, err := os.Open(gzipFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open gzip file: %v", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}

	// Use buffered reader for better performance
	bufferedReader := bufio.NewReaderSize(gzipReader, 64*1024) // 64KB buffer
	decoder := json.NewDecoder(bufferedReader)

	return &StreamingGzipProcessor{
		decoder:    decoder,
		gzipReader: gzipReader,
		file:       file,
	}, nil
}

// Close closes all resources
func (sgp *StreamingGzipProcessor) Close() error {
	var gzipErr, fileErr error

	if sgp.gzipReader != nil {
		gzipErr = sgp.gzipReader.Close()
	}
	if sgp.file != nil {
		fileErr = sgp.file.Close()
	}

	if gzipErr != nil {
		return gzipErr
	}
	return fileErr
}

// ProcessMatches processes the gzip file and writes matching objects to the writer
func (sgp *StreamingGzipProcessor) ProcessMatches(writer *bufio.Writer) (int, error) {
	defer sgp.Close()

	// Check if the JSON starts with an array or object
	firstByte, err := sgp.peekFirstNonWhitespace()
	if err != nil {
		return 0, fmt.Errorf("failed to peek first byte: %v", err)
	}

	matchCount := 0

	if firstByte == '[' {
		// Process as JSON array
		matchCount, err = sgp.processArray(writer)
	} else if firstByte == '{' {
		// Process as single object or stream of objects
		matchCount, err = sgp.processObjects(writer)
	} else {
		return 0, fmt.Errorf("unexpected JSON structure, starts with: %c", firstByte)
	}

	return matchCount, err
}

// peekFirstNonWhitespace looks ahead to find the first non-whitespace character
func (sgp *StreamingGzipProcessor) peekFirstNonWhitespace() (byte, error) {
	// Create a new buffered reader to peek without consuming
	reader := bufio.NewReader(sgp.gzipReader)

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if !unicode.IsSpace(rune(b)) {
			// Put the byte back
			reader.UnreadByte()
			// Update our decoder to use this buffered reader
			sgp.decoder = json.NewDecoder(reader)
			return b, nil
		}
	}
}

// processArray processes a JSON array structure
func (sgp *StreamingGzipProcessor) processArray(writer *bufio.Writer) (int, error) {
	// Consume opening bracket
	token, err := sgp.decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("failed to read opening bracket: %v", err)
	}

	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return 0, fmt.Errorf("expected '[' but got %v", token)
	}

	matchCount := 0
	encoder := json.NewEncoder(writer)

	// Process array elements
	for sgp.decoder.More() {
		var record map[string]interface{}
		if err := sgp.decoder.Decode(&record); err != nil {
			return matchCount, fmt.Errorf("failed to decode record: %v", err)
		}

		// Check if this record matches our criteria
		if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
			if err := encoder.Encode(record); err != nil {
				return matchCount, fmt.Errorf("failed to write match: %v", err)
			}
			matchCount++
		}
	}

	return matchCount, nil
}

// processObjects processes individual JSON objects (single object or stream)
func (sgp *StreamingGzipProcessor) processObjects(writer *bufio.Writer) (int, error) {
	matchCount := 0
	encoder := json.NewEncoder(writer)

	for {
		var record map[string]interface{}
		if err := sgp.decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return matchCount, fmt.Errorf("failed to decode record: %v", err)
		}

		// Check if this record matches our criteria
		if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
			if err := encoder.Encode(record); err != nil {
				return matchCount, fmt.Errorf("failed to write match: %v", err)
			}
			matchCount++
		} else {
			// If the object itself isn't a match, search recursively
			nestedMatches := findMatchingObjectsRecursive(record)
			for _, match := range nestedMatches {
				if err := encoder.Encode(match); err != nil {
					return matchCount, fmt.Errorf("failed to write nested match: %v", err)
				}
				matchCount++
			}
		}
	}

	return matchCount, nil
}

// processJSONFileAndWriteMatches processes regular JSON files (legacy function for non-gzip files) - COMMENTED OUT
// func processJSONFileAndWriteMatches(filePath string, writer *bufio.Writer) (int, error) {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer file.Close()
//
// 	// Use a buffered reader to allow peeking at the first characters without consuming them.
// 	br := bufio.NewReader(file)
// 	var firstChar byte
//
// 	// Loop to skip any leading whitespace and find the first actual character.
// 	for {
// 		b, err := br.ReadByte()
// 		if err != nil {
// 			if err == io.EOF {
// 				return 0, nil // File is empty, which is not an error.
// 			}
// 			return 0, err
// 		}
// 		if !unicode.IsSpace(rune(b)) {
// 			firstChar = b
// 			break
// 		}
// 	}
//
// 	// Put the character back into the reader stream so the JSON decoder can see it.
// 	if err := br.UnreadByte(); err != nil {
// 		return 0, err
// 	}
//
// 	decoder := json.NewDecoder(br)
// 	encoder := json.NewEncoder(writer)
// 	count := 0
//
// 	// Handle file based on whether it's a JSON array or object.
// 	if firstChar == '[' {
// 		// It's an array. Consume the opening '['.
// 		if _, err := decoder.Token(); err != nil {
// 			return 0, fmt.Errorf("failed to read opening `[`: %w", err)
// 		}
//
// 		// Loop through the array elements as long as there are more.
// 		for decoder.More() {
// 			var record map[string]interface{}
// 			if err := decoder.Decode(&record); err != nil {
// 				fmt.Printf("\nWarning: could not decode an object in %s: %v. Skipping object.", filepath.Base(filePath), err)
// 				continue
// 			}
//
// 			if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
// 				if err := encoder.Encode(record); err != nil {
// 					return count, fmt.Errorf("failed to write matched record to output: %w", err)
// 				}
// 				count++
// 			}
// 		}
// 	} else if firstChar == '{' {
// 		// The file starts with an object. It could be a single large object,
// 		// or a stream of objects (JSON Lines format). We'll process it as a stream
// 		// by decoding objects one by one until we reach the end of the file.
// 		for {
// 			var record map[string]interface{}
// 			if err := decoder.Decode(&record); err != nil {
// 				if err == io.EOF {
// 					break // End of file, we're done.
// 				}
// 				// If there's an error, we'll stop processing this file.
// 				fmt.Printf("\nWarning: could not decode an object in %s: %v. File may be malformed.", filepath.Base(filePath), err)
// 				break
// 			}
//
// 			// We have a single decoded object. Check if it's a match.
// 			if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
// 				if err := encoder.Encode(record); err != nil {
// 					return count, fmt.Errorf("failed to write matched record to output: %w", err)
// 				}
// 				count++
// 			} else {
// 				// If the object itself isn't a match, it might contain matches within it.
// 				// This handles cases where records are nested inside a larger structure.
// 				nestedMatches := findMatchingObjectsRecursive(record)
// 				for _, match := range nestedMatches {
// 					if err := encoder.Encode(match); err != nil {
// 						return count, fmt.Errorf("failed to write matched record from object: %w", err)
// 					}
// 					count++
// 				}
// 			}
// 		}
// 	} else {
// 		return 0, fmt.Errorf("file does not appear to be valid JSON (starts with %c)", firstChar)
// 	}
//
// 	return count, nil
// }

// A result struct to pass information back from workers.
type result struct {
	fileName     string
	recordsFound int
	err          error
}

// worker is the function that will be run concurrently.
// It reads file paths from the jobs channel, processes them, and sends the result to the results channel.
func worker(id int, jobs <-chan string, results chan<- result, writer *bufio.Writer, writerMutex *sync.Mutex) {
	for filePath := range jobs {
		// Each worker locks the writer before processing a file to ensure that
		// all writes from a single file are contiguous and not interleaved with other workers.
		writerMutex.Lock()

		// Process gzip files only (JSON file processing commented out)
		var recordsFound int
		var err error

		if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
			// Process gzip file directly with streaming
			processor, procErr := NewStreamingGzipProcessor(filePath)
			if procErr != nil {
				err = fmt.Errorf("failed to create gzip processor: %v", procErr)
				recordsFound = 0
			} else {
				recordsFound, err = processor.ProcessMatches(writer)
			}
		} else {
			// Process regular JSON file (legacy path) - COMMENTED OUT
			// recordsFound, err = processJSONFileAndWriteMatches(filePath, writer)
			err = fmt.Errorf("JSON file processing is disabled - only processing .gz files")
			recordsFound = 0
		}

		// Flush the buffer after each file
		if flushErr := writer.Flush(); flushErr != nil && err == nil {
			err = fmt.Errorf("failed to flush writer: %v", flushErr)
		}

		writerMutex.Unlock()

		results <- result{
			fileName:     filepath.Base(filePath),
			recordsFound: recordsFound,
			err:          err,
		}
	}
}

func main() {
	fmt.Println("Starting optimized streaming JSON parser...")

	// Output file using JSON Lines format
	outputFile := "matches.jsonl"

	// Process both gzip files directly from scraper and decompressed JSON files
	var filesToProcess []string
	processedFiles, err := loadProcessedFiles()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d previously processed files from %s\n", len(processedFiles), processedFilesLog)

	// Process gzip files directly from scraper downloads (preferred)
	gzipDirPath := "../scraper/downloads"
	if gzipFiles, err := os.ReadDir(gzipDirPath); err == nil {
		for _, file := range gzipFiles {
			fileName := file.Name()
			if !file.IsDir() && strings.HasSuffix(strings.ToLower(fileName), ".gz") {
				if !processedFiles[fileName] {
					filesToProcess = append(filesToProcess, filepath.Join(gzipDirPath, fileName))
				}
			}
		}
		fmt.Printf("Found %d new gzip files to process directly\n", len(filesToProcess))
	} else {
		fmt.Printf("Could not access gzip directory %s: %v\n", gzipDirPath, err)
	}

	// Also process any decompressed JSON files as fallback (COMMENTED OUT)
	// initialFileCount := len(filesToProcess)
	// jsonDirPath := "../decompress/output"
	// if jsonFiles, err := os.ReadDir(jsonDirPath); err == nil {
	// 	for _, file := range jsonFiles {
	// 		fileName := file.Name()
	// 		if !file.IsDir() && strings.HasSuffix(strings.ToLower(fileName), ".json") {
	// 			if !processedFiles[fileName] {
	// 				filesToProcess = append(filesToProcess, filepath.Join(jsonDirPath, fileName))
	// 			}
	// 		}
	// 	}
	// 	additionalFiles := len(filesToProcess) - initialFileCount
	// 	fmt.Printf("Found %d additional JSON files to process\n", additionalFiles)
	// } else {
	// 	fmt.Printf("Could not access JSON directory %s: %v\n", jsonDirPath, err)
	// }

	if len(filesToProcess) == 0 {
		fmt.Println("No new files to process.")
		return
	}

	fmt.Printf("Total files to process: %d\n", len(filesToProcess))

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

	// Create a buffered writer for better performance
	bufferedWriter := bufio.NewWriterSize(out, 64*1024) // 64KB buffer
	defer bufferedWriter.Flush()

	// Start workers.
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, results, bufferedWriter, writerMutex)
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
	fmt.Printf("Files skipped (already processed): %d\n", len(processedFiles))

	// Generate CSV output from the .jsonl file
	fmt.Println("\nGenerating CSV output...")
	ExtractToCSV()
}
