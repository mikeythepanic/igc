package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// robustDecompress handles corrupted gzip files by reading as much as possible
func robustDecompress(gzipFile string) error {
	// Open the gzip file
	file, err := os.Open(gzipFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	// Don't defer close - we'll handle errors manually

	// Create output file in the output directory
	baseFileName := filepath.Base(strings.TrimSuffix(gzipFile, ".gz"))
	outputFile := filepath.Join("output", baseFileName)

	// Ensure output directory exists
	if err := os.MkdirAll("output", 0755); err != nil {
		gzipReader.Close()
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	output, err := os.Create(outputFile)
	if err != nil {
		gzipReader.Close()
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	// Read in chunks and handle errors gracefully
	buffer := make([]byte, 8192)
	totalBytes := 0
	chunkCount := 0

	for {
		n, err := gzipReader.Read(buffer)
		if n > 0 {
			_, writeErr := output.Write(buffer[:n])
			if writeErr != nil {
				gzipReader.Close()
				return fmt.Errorf("failed to write to output: %v", writeErr)
			}
			totalBytes += n
			chunkCount++

			// Progress updates
			if chunkCount%1000 == 0 {
				fmt.Printf("Processed %d chunks, %d total bytes\n", chunkCount, totalBytes)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			// If we get an error but have read some data, try to close gracefully
			if totalBytes > 0 {
				fmt.Printf("⚠ Warning: Got error during decompression: %v\n", err)
				fmt.Printf("⚠ But we've already decompressed %d bytes, attempting to save...\n", totalBytes)

				// Try to close the reader - ignore close errors
				closeErr := gzipReader.Close()
				if closeErr != nil {
					fmt.Printf("⚠ Warning: Gzip reader close error (ignored): %v\n", closeErr)
				}

				fmt.Printf("✓ Successfully saved %d bytes to %s (partial decompression)\n", totalBytes, outputFile)
				return nil
			}
			gzipReader.Close()
			return fmt.Errorf("error reading gzip: %v", err)
		}
	}

	// Try to close the reader
	closeErr := gzipReader.Close()
	if closeErr != nil {
		fmt.Printf("⚠ Warning: Gzip reader close error (but decompression succeeded): %v\n", closeErr)
	}

	fmt.Printf("✓ Successfully decompressed %d bytes to %s\n", totalBytes, outputFile)
	return nil
}

// simpleDecompress uses the most basic approach possible
func simpleDecompress(gzipFile string) error {
	// Open the gzip file
	file, err := os.Open(gzipFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	// Don't defer close here - we'll close it manually after reading

	// Create output file in the output directory
	baseFileName := filepath.Base(strings.TrimSuffix(gzipFile, ".gz"))
	outputFile := filepath.Join("output", baseFileName)

	// Ensure output directory exists
	if err := os.MkdirAll("output", 0755); err != nil {
		gzipReader.Close()
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	output, err := os.Create(outputFile)
	if err != nil {
		gzipReader.Close()
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	// Copy data - this is the key part
	bytesWritten, err := io.Copy(output, gzipReader)
	if err != nil {
		gzipReader.Close()
		return fmt.Errorf("failed to copy data: %v", err)
	}

	// Close the gzip reader AFTER copying
	err = gzipReader.Close()
	if err != nil {
		return fmt.Errorf("gzip reader close error: %v", err)
	}

	fmt.Printf("✓ Successfully decompressed %d bytes to %s\n", bytesWritten, outputFile)
	return nil
}

// readGzippedJSON reads a gzipped JSON file and validates the JSON structure
func readGzippedJSON(filename string) ([]byte, error) {
	// Open the gzip file
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	// Read all decompressed data
	data, err := io.ReadAll(gzipReader)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("unexpected EOF - gzip file may be corrupted or incomplete: %v", err)
		}
		return nil, fmt.Errorf("failed to read decompressed data: %v", err)
	}

	// Verify the gzip reader closed properly
	if err := gzipReader.Close(); err != nil {
		return nil, fmt.Errorf("gzip reader close error (file may be corrupted): %v", err)
	}

	// Validate that it's valid JSON
	if len(data) > 0 {
		var jsonTest interface{}
		if err := json.Unmarshal(data, &jsonTest); err != nil {
			return nil, fmt.Errorf("decompressed data is not valid JSON: %v", err)
		}
		fmt.Printf("✓ Valid JSON structure detected\n")
	}

	return data, nil
}

// decompressGzipToFile decompresses a gzip file to a new file
func decompressGzipToFile(gzipFile, outputFile string) error {
	// Open the gzip file
	file, err := os.Open(gzipFile)
	if err != nil {
		return fmt.Errorf("failed to open gzip file: %v", err)
	}
	defer file.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	// Create output file
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	// Copy decompressed data to output file
	bytesWritten, err := io.Copy(output, gzipReader)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return fmt.Errorf("unexpected EOF during decompression - file may be corrupted: %v", err)
		}
		return fmt.Errorf("failed to write decompressed data: %v", err)
	}

	// Verify the gzip reader close properly
	if err := gzipReader.Close(); err != nil {
		return fmt.Errorf("gzip reader close error (file may be corrupted): %v", err)
	}

	fmt.Printf("Wrote %d bytes to %s\n", bytesWritten, outputFile)
	return nil
}

// processGzipStream processes a gzip file as a stream (good for large files)
func processGzipStream(filename string) error {
	// Open the gzip file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	// Process the stream in chunks
	buffer := make([]byte, 4096) // 4KB chunks
	totalBytes := 0
	chunkCount := 0

	for {
		n, err := gzipReader.Read(buffer)
		if n > 0 {
			totalBytes += n
			chunkCount++

			// Process the chunk here (example: just count bytes)
			// In a real application, you might parse JSON, search for patterns, etc.
			if chunkCount%1000 == 0 {
				fmt.Printf("Processed %d chunks, %d total bytes\n", chunkCount, totalBytes)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf("unexpected EOF during stream processing - file may be corrupted: %v", err)
			}
			return fmt.Errorf("error reading stream: %v", err)
		}
	}

	// Verify the gzip reader close properly
	if err := gzipReader.Close(); err != nil {
		return fmt.Errorf("gzip reader close error (file may be corrupted): %v", err)
	}

	fmt.Printf("Stream processing complete: %d chunks, %d total bytes\n", chunkCount, totalBytes)
	return nil
}

func main() {
	// Process all gzip files in the downloads directory
	downloadsDir := "../scraper/downloads"

	fmt.Printf("Scanning directory: %s\n", downloadsDir)

	// Find all .gz files in the directory
	gzipFiles, err := findGzipFiles(downloadsDir)
	if err != nil {
		fmt.Printf("Error scanning directory: %v\n", err)
		return
	}

	fmt.Printf("Found %d gzip files to process\n", len(gzipFiles))

	// Process each file
	successCount := 0
	errorCount := 0
	partialCount := 0

	for i, gzipFile := range gzipFiles {
		fmt.Printf("\n[%d/%d] Processing: %s\n", i+1, len(gzipFiles), filepath.Base(gzipFile))

		// Try simple decompression first
		err := simpleDecompress(gzipFile)
		if err != nil {
			fmt.Printf("⚠ Simple decompression failed: %v\n", err)
			fmt.Printf("🔄 Trying robust decompression...\n")

			// Fall back to robust decompression
			err = robustDecompress(gzipFile)
			if err != nil {
				fmt.Printf("❌ Both decompression methods failed for %s: %v\n", filepath.Base(gzipFile), err)
				errorCount++
				continue
			} else {
				fmt.Printf("⚠ Robust decompression completed (may be partial)\n")
				partialCount++
			}
		} else {
			fmt.Printf("✅ Simple decompression successful\n")
			successCount++
		}

		// Validate the JSON output
		baseFileName := filepath.Base(strings.TrimSuffix(gzipFile, ".gz"))
		outputFile := filepath.Join("output", baseFileName)
		if isValidJSON(outputFile) {
			fmt.Printf("✅ JSON validation passed\n")
		} else {
			fmt.Printf("⚠ JSON validation failed - file may be incomplete\n")
			if successCount > 0 {
				successCount--
			}
			partialCount++
		}
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total files: %d\n", len(gzipFiles))
	fmt.Printf("Complete & Valid: %d\n", successCount)
	fmt.Printf("Partial/Invalid: %d\n", partialCount)
	fmt.Printf("Failed: %d\n", errorCount)

	if partialCount > 0 {
		fmt.Printf("\n⚠ Warning: %d files may have incomplete JSON due to gzip corruption\n", partialCount)
		fmt.Printf("These files may cause 'unexpected end of JSON input' errors in your pipeline\n")
	}
}

// isValidJSON checks if a file contains valid JSON
func isValidJSON(filename string) bool {
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

// findGzipFiles recursively finds all .gz files in a directory
func findGzipFiles(dir string) ([]string, error) {
	var gzipFiles []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-.gz files
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(info.Name()), ".gz") {
			return nil
		}

		// Skip empty files
		if info.Size() == 0 {
			fmt.Printf("⚠ Skipping empty file: %s\n", filepath.Base(path))
			return nil
		}

		gzipFiles = append(gzipFiles, path)
		return nil
	})

	return gzipFiles, err
}
