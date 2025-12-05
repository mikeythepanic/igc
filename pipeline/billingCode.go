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
	fmt.Printf("DEBUG: Loading processed files from %s\n", processedFilesLog)
	files := make(map[string]bool)
	file, err := os.Open(processedFilesLog)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("DEBUG: Processed files log does not exist yet")
			return files, nil // No log yet
		}
		fmt.Printf("DEBUG: Error opening processed files log: %v\n", err)
		return nil, err
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("DEBUG: Error getting file stats for processed files log: %v\n", err)
		return nil, err
	}
	fmt.Printf("DEBUG: Processed files log size: %d bytes\n", fileInfo.Size())
	if fileInfo.Size() == 0 {
		fmt.Println("DEBUG: Processed files log is empty")
		return files, nil // Empty log, treat as no files processed
	}

	var fileList []string
	fmt.Println("DEBUG: Attempting to decode processed files JSON")
	if err := json.NewDecoder(file).Decode(&fileList); err != nil {
		// If the file contains an empty array "[]", it's valid but we should handle it
		if err == io.EOF {
			fmt.Println("DEBUG: Got EOF when decoding processed files - treating as empty")
			return files, nil
		}
		fmt.Printf("DEBUG: Error decoding processed files JSON: %v\n", err)
		return nil, err
	}
	fmt.Printf("DEBUG: Successfully loaded %d processed files\n", len(fileList))
	for _, f := range fileList {
		files[f] = true
	}
	return files, nil
}

// saveProcessedFiles saves the set of processed files to the log
func saveProcessedFiles(files map[string]bool) error {
	fmt.Printf("DEBUG: [SAVE_LOG] Saving %d processed files to %s\n", len(files), processedFilesLog)
	fileList := make([]string, 0, len(files))
	for f := range files {
		fileList = append(fileList, f)
	}
	fmt.Printf("DEBUG: [SAVE_LOG] Creating processed files log: %s\n", processedFilesLog)
	file, err := os.Create(processedFilesLog)
	if err != nil {
		fmt.Printf("DEBUG: [IO_ERROR] Failed to create processed files log: %v (error type: %T)\n", err, err)
		return err
	}
	defer file.Close()
	fmt.Println("DEBUG: [SAVE_LOG] Successfully created file, setting up JSON encoder")
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	fmt.Println("DEBUG: [SAVE_LOG] Attempting to marshal and write JSON")
	if err := encoder.Encode(fileList); err != nil {
		fmt.Printf("DEBUG: [MARSHAL_ERROR] Failed to marshal processed files list: %v (error type: %T)\n", err, err)
		return err
	}
	fmt.Printf("DEBUG: [SAVE_LOG] Successfully saved %d processed files to log\n", len(fileList))
	return nil
}

// StreamingGzipProcessor provides streaming processing of gzip files
type StreamingGzipProcessor struct {
	decoder    *json.Decoder
	gzipReader *gzip.Reader
	file       *os.File
}

// NewStreamingGzipProcessor creates a new streaming processor for gzip files
func NewStreamingGzipProcessor(gzipFilePath string) (*StreamingGzipProcessor, error) {
	fmt.Printf("DEBUG: [FILE_OPEN] Attempting to open gzip file: %s\n", gzipFilePath)

	// Check if file exists and get stats
	fileInfo, statErr := os.Stat(gzipFilePath)
	if statErr != nil {
		fmt.Printf("DEBUG: [FILE_OPEN] Error getting file stats for %s: %v\n", gzipFilePath, statErr)
		return nil, fmt.Errorf("failed to get file stats: %v", statErr)
	}
	fmt.Printf("DEBUG: [FILE_OPEN] File %s exists, size: %d bytes\n", gzipFilePath, fileInfo.Size())

	if fileInfo.Size() == 0 {
		fmt.Printf("DEBUG: [EMPTY_FILE] File %s is empty (0 bytes)\n", gzipFilePath)
		return nil, fmt.Errorf("file is empty")
	}

	file, err := os.Open(gzipFilePath)
	if err != nil {
		fmt.Printf("DEBUG: [FILE_OPEN] Failed to open gzip file %s: %v (error type: %T)\n", gzipFilePath, err, err)
		return nil, fmt.Errorf("failed to open gzip file: %v", err)
	}
	fmt.Printf("DEBUG: [FILE_OPEN] Successfully opened file handle for: %s\n", gzipFilePath)

	fmt.Printf("DEBUG: [GZIP_READER] Creating gzip reader for: %s\n", gzipFilePath)
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Printf("DEBUG: [GZIP_READER] Failed to create gzip reader for %s: %v (error type: %T)\n", gzipFilePath, err, err)
		fmt.Printf("DEBUG: [BROKEN_FILE] File %s may be corrupted or not a valid gzip file\n", gzipFilePath)
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	fmt.Printf("DEBUG: [GZIP_READER] Successfully created gzip reader for: %s\n", gzipFilePath)

	// Use buffered reader for better performance
	bufferedReader := bufio.NewReaderSize(gzipReader, 64*1024) // 64KB buffer
	decoder := json.NewDecoder(bufferedReader)
	fmt.Printf("DEBUG: [JSON_DECODER] Created JSON decoder with 64KB buffer for: %s\n", gzipFilePath)

	fmt.Printf("DEBUG: [SUCCESS] Successfully created streaming processor for: %s\n", gzipFilePath)
	return &StreamingGzipProcessor{
		decoder:    decoder,
		gzipReader: gzipReader,
		file:       file,
	}, nil
}

// Close closes all resources
func (sgp *StreamingGzipProcessor) Close() error {
	fmt.Println("DEBUG: [CLOSE] Starting to close streaming processor resources")
	var gzipErr, fileErr error

	if sgp.gzipReader != nil {
		fmt.Println("DEBUG: [CLOSE] Closing gzip reader")
		gzipErr = sgp.gzipReader.Close()
		if gzipErr != nil {
			fmt.Printf("DEBUG: [IO_ERROR] Error closing gzip reader: %v (error type: %T)\n", gzipErr, gzipErr)
		} else {
			fmt.Println("DEBUG: [CLOSE] Successfully closed gzip reader")
		}
	}
	if sgp.file != nil {
		fmt.Println("DEBUG: [CLOSE] Closing file handle")
		fileErr = sgp.file.Close()
		if fileErr != nil {
			fmt.Printf("DEBUG: [IO_ERROR] Error closing file: %v (error type: %T)\n", fileErr, fileErr)
		} else {
			fmt.Println("DEBUG: [CLOSE] Successfully closed file handle")
		}
	}

	if gzipErr != nil {
		fmt.Printf("DEBUG: [CLOSE] Returning gzip close error: %v\n", gzipErr)
		return gzipErr
	}
	if fileErr != nil {
		fmt.Printf("DEBUG: [CLOSE] Returning file close error: %v\n", fileErr)
	} else {
		fmt.Println("DEBUG: [CLOSE] All resources closed successfully")
	}
	return fileErr
}

// ProcessMatches processes the gzip file and writes matching objects to the writer
func (sgp *StreamingGzipProcessor) ProcessMatches(writer *bufio.Writer) (int, error) {
	fmt.Println("DEBUG: [PROCESS] Starting ProcessMatches")
	defer func() {
		fmt.Println("DEBUG: [PROCESS] Closing streaming processor resources")
		sgp.Close()
	}()

	// Check if the JSON starts with an array or object
	fmt.Println("DEBUG: [PROCESS] Determining JSON structure type")
	firstByte, err := sgp.peekFirstNonWhitespace()
	if err != nil {
		fmt.Printf("DEBUG: [PROCESS] Failed to determine JSON structure: %v\n", err)
		return 0, fmt.Errorf("failed to peek first byte: %v", err)
	}

	matchCount := 0
	fmt.Printf("DEBUG: [PROCESS] JSON structure determined: starts with '%c'\n", firstByte)

	if firstByte == '[' {
		fmt.Println("DEBUG: [PROCESS] Processing as JSON array")
		matchCount, err = sgp.processArray(writer)
		if err != nil {
			fmt.Printf("DEBUG: [PROCESS] Array processing failed: %v\n", err)
		} else {
			fmt.Printf("DEBUG: [PROCESS] Array processing completed successfully, %d matches\n", matchCount)
		}
	} else if firstByte == '{' {
		fmt.Println("DEBUG: [PROCESS] Processing as JSON object stream")
		matchCount, err = sgp.processObjects(writer)
		if err != nil {
			fmt.Printf("DEBUG: [PROCESS] Object processing failed: %v\n", err)
		} else {
			fmt.Printf("DEBUG: [PROCESS] Object processing completed successfully, %d matches\n", matchCount)
		}
	} else {
		fmt.Printf("DEBUG: [PROCESS] Unexpected JSON structure, starts with: %c (ASCII: %d)\n", firstByte, firstByte)
		return 0, fmt.Errorf("unexpected JSON structure, starts with: %c", firstByte)
	}

	fmt.Printf("DEBUG: [PROCESS] ProcessMatches completed with %d matches and error: %v\n", matchCount, err)
	return matchCount, err
}

// peekFirstNonWhitespace looks ahead to find the first non-whitespace character
func (sgp *StreamingGzipProcessor) peekFirstNonWhitespace() (byte, error) {
	fmt.Println("DEBUG: [PEEK] Starting to peek for first non-whitespace character")
	// Create a new buffered reader to peek without consuming
	reader := bufio.NewReader(sgp.gzipReader)
	byteCount := 0

	for {
		byteCount++
		fmt.Printf("DEBUG: [PEEK] Reading byte #%d\n", byteCount)
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				fmt.Printf("DEBUG: [EOF] Got EOF while peeking for first character after %d bytes\n", byteCount-1)
				fmt.Println("DEBUG: [EMPTY_FILE] File appears to be empty or only contains whitespace")
			} else {
				fmt.Printf("DEBUG: [IO_ERROR] I/O error while peeking for first character: %v (error type: %T)\n", err, err)
			}
			return 0, err
		}
		fmt.Printf("DEBUG: [PEEK] Read byte #%d: %q (ASCII: %d)\n", byteCount, rune(b), b)
		if !unicode.IsSpace(rune(b)) {
			fmt.Printf("DEBUG: [PEEK] Found first non-whitespace character: %c (ASCII: %d) after %d bytes\n", b, b, byteCount)
			// Put the byte back
			if unreadErr := reader.UnreadByte(); unreadErr != nil {
				fmt.Printf("DEBUG: [IO_ERROR] Failed to unread byte: %v\n", unreadErr)
				return 0, unreadErr
			}
			// Update our decoder to use this buffered reader
			sgp.decoder = json.NewDecoder(reader)
			fmt.Println("DEBUG: [PEEK] Successfully updated JSON decoder with buffered reader")
			return b, nil
		}
		if byteCount > 1000 {
			fmt.Printf("DEBUG: [WARNING] Read %d whitespace characters, file may have excessive leading whitespace\n", byteCount)
		}
	}
}

// processArray processes a JSON array structure
func (sgp *StreamingGzipProcessor) processArray(writer *bufio.Writer) (int, error) {
	fmt.Println("DEBUG: [ARRAY] Starting to process JSON array")
	// Consume opening bracket
	fmt.Println("DEBUG: [ARRAY] Attempting to read opening bracket token")
	token, err := sgp.decoder.Token()
	if err != nil {
		if err == io.EOF {
			fmt.Println("DEBUG: [EOF] Got EOF when trying to read opening bracket - array may be empty")
		} else {
			fmt.Printf("DEBUG: [JSON_ERROR] Error reading opening bracket: %v (error type: %T)\n", err, err)
		}
		return 0, fmt.Errorf("failed to read opening bracket: %v", err)
	}

	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		fmt.Printf("DEBUG: [JSON_ERROR] Expected '[' but got %v (type: %T)\n", token, token)
		return 0, fmt.Errorf("expected '[' but got %v", token)
	}

	fmt.Println("DEBUG: [ARRAY] Successfully consumed opening bracket")
	matchCount := 0
	encoder := json.NewEncoder(writer)
	elementCount := 0

	// Process array elements
	fmt.Println("DEBUG: [ARRAY] Starting to iterate through array elements")
	for sgp.decoder.More() {
		elementCount++
		fmt.Printf("DEBUG: [ARRAY] Processing array element #%d\n", elementCount)
		var record map[string]interface{}
		if err := sgp.decoder.Decode(&record); err != nil {
			if err == io.EOF {
				fmt.Printf("DEBUG: [EOF] Got EOF while decoding array element #%d\n", elementCount)
			} else {
				fmt.Printf("DEBUG: [JSON_ERROR] Error decoding array element #%d: %v (error type: %T)\n", elementCount, err, err)
				fmt.Printf("DEBUG: [BROKEN_FILE] Array element #%d may be malformed JSON\n", elementCount)
			}
			return matchCount, fmt.Errorf("failed to decode record: %v", err)
		}

		fmt.Printf("DEBUG: [ARRAY] Successfully decoded array element #%d (keys: %d)\n", elementCount, len(record))

		// Check if this record matches our criteria
		if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
			fmt.Printf("DEBUG: [MATCH] Found matching billing code %s in array element #%d\n", billingCode, elementCount)
			if err := encoder.Encode(record); err != nil {
				fmt.Printf("DEBUG: [MARSHAL_ERROR] Error marshaling/writing match for array element #%d: %v (error type: %T)\n", elementCount, err, err)
				return matchCount, fmt.Errorf("failed to write match: %v", err)
			}
			fmt.Printf("DEBUG: [MATCH] Successfully wrote match #%d to output\n", matchCount+1)
			matchCount++
		} else {
			fmt.Printf("DEBUG: [ARRAY] No billing_code match in element #%d (has billing_code: %t)\n", elementCount, record["billing_code"] != nil)
		}

		if elementCount%1000 == 0 {
			fmt.Printf("DEBUG: [PROGRESS] Processed %d array elements so far, %d matches found\n", elementCount, matchCount)
		}
	}

	fmt.Printf("DEBUG: [ARRAY] Finished processing array. Total elements: %d, Total matches: %d\n", elementCount, matchCount)
	return matchCount, nil
}

// processObjects processes individual JSON objects (single object or stream)
func (sgp *StreamingGzipProcessor) processObjects(writer *bufio.Writer) (int, error) {
	fmt.Println("DEBUG: [OBJECTS] Starting to process JSON objects")
	matchCount := 0
	encoder := json.NewEncoder(writer)
	objectCount := 0

	for {
		objectCount++
		fmt.Printf("DEBUG: [OBJECTS] Attempting to decode object #%d\n", objectCount)
		var record map[string]interface{}
		if err := sgp.decoder.Decode(&record); err != nil {
			if err == io.EOF {
				fmt.Printf("DEBUG: [EOF] Reached EOF after processing %d objects (normal end of stream)\n", objectCount-1)
				break
			} else {
				fmt.Printf("DEBUG: [JSON_ERROR] Error decoding object #%d: %v (error type: %T)\n", objectCount, err, err)
				fmt.Printf("DEBUG: [BROKEN_FILE] Object #%d may be malformed JSON or file corruption\n", objectCount)
			}
			return matchCount, fmt.Errorf("failed to decode record: %v", err)
		}

		fmt.Printf("DEBUG: [OBJECTS] Successfully decoded object #%d (keys: %d)\n", objectCount, len(record))

		// Check if this record matches our criteria
		if billingCode, exists := record["billing_code"].(string); exists && targetCodes[billingCode] {
			fmt.Printf("DEBUG: [MATCH] Found matching billing code %s in object #%d\n", billingCode, objectCount)
			if err := encoder.Encode(record); err != nil {
				fmt.Printf("DEBUG: [MARSHAL_ERROR] Error marshaling/writing match for object #%d: %v (error type: %T)\n", objectCount, err, err)
				return matchCount, fmt.Errorf("failed to write match: %v", err)
			}
			fmt.Printf("DEBUG: [MATCH] Successfully wrote direct match #%d to output\n", matchCount+1)
			matchCount++
		} else {
			// If the object itself isn't a match, search recursively
			fmt.Printf("DEBUG: [OBJECTS] No direct billing_code match in object #%d (has billing_code: %t), searching recursively\n", objectCount, record["billing_code"] != nil)
			nestedMatches := findMatchingObjectsRecursive(record)
			if len(nestedMatches) > 0 {
				fmt.Printf("DEBUG: [NESTED] Found %d nested matches in object #%d\n", len(nestedMatches), objectCount)
				for i, match := range nestedMatches {
					fmt.Printf("DEBUG: [NESTED] Processing nested match %d/%d from object #%d\n", i+1, len(nestedMatches), objectCount)
					if err := encoder.Encode(match); err != nil {
						fmt.Printf("DEBUG: [MARSHAL_ERROR] Error marshaling/writing nested match %d for object #%d: %v (error type: %T)\n", i+1, objectCount, err, err)
						return matchCount, fmt.Errorf("failed to write nested match: %v", err)
					}
					fmt.Printf("DEBUG: [MATCH] Successfully wrote nested match #%d to output\n", matchCount+1)
					matchCount++
				}
			} else {
				fmt.Printf("DEBUG: [OBJECTS] No nested matches found in object #%d\n", objectCount)
			}
		}

		if objectCount%500 == 0 {
			fmt.Printf("DEBUG: [PROGRESS] Processed %d objects so far, %d matches found\n", objectCount, matchCount)
		}
	}

	fmt.Printf("DEBUG: [OBJECTS] Finished processing objects. Total objects: %d, Total matches: %d\n", objectCount-1, matchCount)
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
		fmt.Printf("DEBUG: Worker %d starting to process file: %s\n", id, filePath)
		// Each worker locks the writer before processing a file to ensure that
		// all writes from a single file are contiguous and not interleaved with other workers.
		writerMutex.Lock()

		// Process gzip files only (JSON file processing commented out)
		var recordsFound int
		var err error

		if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
			fmt.Printf("DEBUG: Worker %d processing gzip file: %s\n", id, filePath)
			// Process gzip file directly with streaming
			processor, procErr := NewStreamingGzipProcessor(filePath)
			if procErr != nil {
				fmt.Printf("DEBUG: Worker %d failed to create processor for %s: %v\n", id, filePath, procErr)
				err = fmt.Errorf("failed to create gzip processor: %v", procErr)
				recordsFound = 0
			} else {
				fmt.Printf("DEBUG: Worker %d starting to process matches for: %s\n", id, filePath)
				recordsFound, err = processor.ProcessMatches(writer)
				if err != nil {
					fmt.Printf("DEBUG: Worker %d error processing matches for %s: %v\n", id, filePath, err)
				} else {
					fmt.Printf("DEBUG: Worker %d successfully processed %s, found %d records\n", id, filePath, recordsFound)
				}
			}
		} else {
			fmt.Printf("DEBUG: Worker %d skipping non-gzip file: %s\n", id, filePath)
			// Process regular JSON file (legacy path) - COMMENTED OUT
			// recordsFound, err = processJSONFileAndWriteMatches(filePath, writer)
			err = fmt.Errorf("JSON file processing is disabled - only processing .gz files")
			recordsFound = 0
		}

		// Flush the buffer after each file
		fmt.Printf("DEBUG: [WORKER_%d] Flushing buffered writer for: %s\n", id, filePath)
		if flushErr := writer.Flush(); flushErr != nil && err == nil {
			fmt.Printf("DEBUG: [IO_ERROR] Worker %d flush error for %s: %v (error type: %T)\n", id, filePath, flushErr, flushErr)
			err = fmt.Errorf("failed to flush writer: %v", flushErr)
		} else {
			fmt.Printf("DEBUG: [WORKER_%d] Successfully flushed buffer for: %s\n", id, filePath)
		}

		writerMutex.Unlock()
		fmt.Printf("DEBUG: Worker %d finished processing file: %s\n", id, filePath)

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
	gzipDirPath := "../scraper/downloads2"
	fmt.Printf("DEBUG: Attempting to read gzip directory: %s\n", gzipDirPath)
	if gzipFiles, err := os.ReadDir(gzipDirPath); err == nil {
		fmt.Printf("DEBUG: Successfully read directory, found %d entries\n", len(gzipFiles))
		gzipCount := 0
		processedCount := 0
		for _, file := range gzipFiles {
			fileName := file.Name()
			if !file.IsDir() && strings.HasSuffix(strings.ToLower(fileName), ".gz") {
				gzipCount++
				if !processedFiles[fileName] {
					filesToProcess = append(filesToProcess, filepath.Join(gzipDirPath, fileName))
				} else {
					processedCount++
					fmt.Printf("DEBUG: Skipping already processed file: %s\n", fileName)
				}
			}
		}
		fmt.Printf("DEBUG: Found %d total gzip files, %d already processed, %d new files to process\n", gzipCount, processedCount, len(filesToProcess))
		fmt.Printf("Found %d new gzip files to process directly\n", len(filesToProcess))
	} else {
		fmt.Printf("DEBUG: Error reading gzip directory %s: %v\n", gzipDirPath, err)
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
	fmt.Printf("DEBUG: [OUTPUT] Opening output file: %s\n", outputFile)
	out, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("DEBUG: [IO_ERROR] Failed to open output file: %v (error type: %T)\n", err, err)
		panic(err)
	}
	defer func() {
		fmt.Println("DEBUG: [OUTPUT] Closing output file")
		out.Close()
	}()

	// Create a buffered writer for better performance
	fmt.Println("DEBUG: [OUTPUT] Creating 64KB buffered writer")
	bufferedWriter := bufio.NewWriterSize(out, 64*1024) // 64KB buffer
	defer func() {
		fmt.Println("DEBUG: [OUTPUT] Final flush of buffered writer")
		if flushErr := bufferedWriter.Flush(); flushErr != nil {
			fmt.Printf("DEBUG: [IO_ERROR] Error during final flush: %v\n", flushErr)
		}
	}()

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
	fmt.Printf("DEBUG: [RESULTS] Starting to collect results from %d workers\n", numWorkers)
	totalNewRecords := 0
	filesProcessed := 0
	errorsCount := 0
	for i := 0; i < len(filesToProcess); i++ {
		fmt.Printf("DEBUG: [RESULTS] Waiting for result %d/%d\n", i+1, len(filesToProcess))
		res := <-results
		filesProcessed++
		fmt.Printf("DEBUG: [RESULTS] Received result for %s: %d records, error: %v\n", res.fileName, res.recordsFound, res.err != nil)
		if res.err != nil {
			errorsCount++
			fmt.Printf("\n[%d/%d] Error processing %s: %v", filesProcessed, len(filesToProcess), res.fileName, res.err)
			fmt.Printf("DEBUG: [ERROR] Error details for %s: %v (error type: %T)\n", res.fileName, res.err, res.err)
		} else {
			if res.recordsFound > 0 {
				fmt.Printf("\n[%d/%d] Processed %s, found %d records.", filesProcessed, len(filesToProcess), res.fileName, res.recordsFound)
				totalNewRecords += res.recordsFound
			} else {
				fmt.Printf("DEBUG: [RESULTS] File %s processed successfully but no records found\n", res.fileName)
			}
			// Mark file as processed in memory
			processedFiles[res.fileName] = true
			fmt.Printf("DEBUG: [RESULTS] Marked %s as processed\n", res.fileName)
		}
	}
	fmt.Println() // Newline after progress updates.
	fmt.Printf("DEBUG: [RESULTS] Results collection complete. Processed: %d, Errors: %d, Total records: %d\n", filesProcessed, errorsCount, totalNewRecords)

	// Save the processed files log once at the end
	fmt.Println("DEBUG: [MAIN] Saving processed files log at end of processing")
	if err := saveProcessedFiles(processedFiles); err != nil {
		fmt.Printf("\nWarning: could not update processed files log: %v\n", err)
		fmt.Printf("DEBUG: [ERROR] Failed to save processed files log: %v (error type: %T)\n", err, err)
	} else {
		fmt.Println("DEBUG: [MAIN] Successfully saved processed files log")
	}

	fmt.Printf("\nProcessing complete!\n")
	fmt.Printf("Total new records added: %d\n", totalNewRecords)
	fmt.Printf("Files processed in this run: %d\n", filesProcessed)
	fmt.Printf("Files skipped (already processed): %d\n", len(processedFiles))

	// Generate CSV output from the .jsonl file
	fmt.Println("\nGenerating CSV output...")
	ExtractToCSV()
}
