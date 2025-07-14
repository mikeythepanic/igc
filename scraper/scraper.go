package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DownloadResult represents the result of a download
type DownloadResult struct {
	URL      string
	Success  bool
	Error    error
	FilePath string
}

// Global HTTP client for reuse
var httpClient *http.Client

func init() {
	// Create a highly optimized HTTP client for bulk downloads
	httpClient = &http.Client{
		Timeout: 60 * time.Second, // Longer timeout for large files
		Transport: &http.Transport{
			MaxIdleConns:          300, // Much higher connection pool
			MaxIdleConnsPerHost:   100, // More connections per host
			MaxConnsPerHost:       150, // Higher total connections per host
			IdleConnTimeout:       120 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DisableCompression:    false,
			WriteBufferSize:       64 * 1024, // 64KB write buffer
			ReadBufferSize:        64 * 1024, // 64KB read buffer
		},
	}
}

func optimalConcurrency() int {
	cores := runtime.NumCPU()

	// For your 6C/12T Ryzen 5 5600x, be more aggressive
	if cores >= 6 {
		return 80 // High concurrency for your hardware
	}

	// Fallback for other systems
	baseConcurrency := cores * 8 // More aggressive multiplier

	min := 20
	max := 100

	if baseConcurrency < min {
		return min
	}
	if baseConcurrency > max {
		return max
	}
	return baseConcurrency
}

// loadURLsFromFile reads URLs from a text file (one URL per line)
func loadURLsFromFile(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		// Skip empty lines and comments
		if line != "" && !strings.HasPrefix(line, "#") {
			// Fix Unicode escapes and check if it's a URL
			cleanedURL := fixUnicodeEscapes(line)
			if isURL(cleanedURL) {
				urls = append(urls, cleanedURL)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return urls, nil
}

// fixUnicodeEscapes converts Unicode escapes to actual characters
func fixUnicodeEscapes(str string) string {
	// Replace the most common Unicode escapes in URLs
	str = strings.ReplaceAll(str, "\\u0026", "&")
	str = strings.ReplaceAll(str, "\\u003d", "=")
	str = strings.ReplaceAll(str, "\\u003f", "?")
	str = strings.ReplaceAll(str, "\\u007e", "~")
	return str
}

// isURL checks if a string is a valid URL
func isURL(str string) bool {
	return strings.HasPrefix(str, "http://") || strings.HasPrefix(str, "https://")
}

func main() {
	fmt.Println("Starting URL Downloader...")
	fmt.Printf("Hardware: %d CPU cores detected\n", runtime.NumCPU())

	// Read URLs from file
	urlFile := "urls.txt" // Fixed path - file is in same directory
	if len(os.Args) > 1 {
		urlFile = os.Args[1]
	}

	fmt.Printf("Reading URLs from: %s\n", urlFile)
	urls, err := loadURLsFromFile(urlFile)
	if err != nil {
		fmt.Printf("Error reading URL file: %v\n", err)
		fmt.Println("Usage: ./scraper [urls.txt]")
		fmt.Println("Create a urls.txt file with one URL per line")
		os.Exit(1)
	}

	fmt.Printf("Found %d URLs to download\n", len(urls))

	if len(urls) == 0 {
		fmt.Println("No valid URLs found in the file")
		return
	}

	// Create downloads directory
	downloadDir := "downloads"
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		fmt.Printf("Error creating downloads directory: %v\n", err)
		os.Exit(1)
	}

	// Check existing files
	existingFiles := countExistingFiles(downloadDir)
	fmt.Printf("Found %d existing files in downloads directory\n", existingFiles)

	// Calculate optimal concurrency
	concurrency := optimalConcurrency()
	fmt.Printf("Using %d concurrent downloads\n", concurrency)

	// Show initial progress
	fmt.Printf("Starting download process...\n")
	fmt.Printf("Progress: 0.0%% (0/%d)\n", len(urls))

	// Pre-check existing files in batch for faster processing
	fmt.Println("Pre-checking existing files...")
	existingFileMap := buildExistingFileMap(downloadDir)

	// Download files with optimal concurrency
	results := downloadFiles(urls, downloadDir, concurrency, existingFileMap)

	// Print summary
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		} else {
			fmt.Printf("Failed to download %s: %v\n", result.URL, result.Error)
		}
	}

	fmt.Printf("\nDownload Summary:\n")
	fmt.Printf("Total URLs: %d\n", len(urls))
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", len(urls)-successCount)
	fmt.Printf("Success Rate: %.1f%%\n", float64(successCount)/float64(len(urls))*100)
	fmt.Printf("Files saved to: %s/\n", downloadDir)
}

// downloadFiles downloads multiple files concurrently
func downloadFiles(urls []string, downloadDir string, concurrency int, existingFileMap map[string]bool) []DownloadResult {
	results := make([]DownloadResult, len(urls))
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	// Progress tracking
	var completed int32
	total := len(urls)

	// Progress channel for updates
	progressChan := make(chan int, total)

	// Batch progress display goroutine (update every 100 downloads for better performance)
	go func() {
		ticker := time.NewTicker(2 * time.Second) // Update every 2 seconds instead of every download
		defer ticker.Stop()

		for {
			select {
			case <-progressChan:
				atomic.AddInt32(&completed, 1)
			case <-ticker.C:
				current := atomic.LoadInt32(&completed)
				percentage := float64(current) / float64(total) * 100
				fmt.Printf("\rProgress: %.1f%% (%d/%d)", percentage, current, total)
			}

			// Check if we're done
			if atomic.LoadInt32(&completed) >= int32(total) {
				current := atomic.LoadInt32(&completed)
				percentage := float64(current) / float64(total) * 100
				fmt.Printf("\rProgress: %.1f%% (%d/%d)", percentage, current, total)
				fmt.Println() // New line after final progress
				return
			}
		}
	}()

	for i, urlString := range urls {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			result := downloadFile(url, downloadDir, existingFileMap)
			results[index] = result

			// Send progress update
			progressChan <- 1
		}(i, urlString)
	}

	wg.Wait()
	close(progressChan) // Close channel to stop progress goroutine

	return results
}

// downloadFile downloads a single file with optimized I/O
func downloadFile(urlString string, downloadDir string, existingFileMap map[string]bool) DownloadResult {
	result := DownloadResult{URL: urlString}

	// Create filename from URL
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		result.Error = fmt.Errorf("invalid URL: %v", err)
		return result
	}

	// Extract filename from URL path
	pathParts := strings.Split(parsedURL.Path, "/")
	filename := pathParts[len(pathParts)-1]
	if filename == "" {
		filename = "unknown_file"
	}

	filePath := filepath.Join(downloadDir, filename)

	// Check if file already exists using the pre-built map (much faster)
	if existingFileMap[filename] {
		result.Success = true
		result.FilePath = filePath
		return result
	}

	// Download the file using the optimized HTTP client
	resp, err := httpClient.Get(urlString)
	if err != nil {
		result.Error = fmt.Errorf("HTTP request failed: %v", err)
		return result
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP status %d", resp.StatusCode)
		return result
	}

	// Create the file with larger buffer for better I/O performance
	file, err := os.Create(filePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to create file: %v", err)
		return result
	}
	defer file.Close()

	// Use a larger buffer for faster copying (1MB buffer)
	buffer := make([]byte, 1024*1024)
	_, err = io.CopyBuffer(file, resp.Body, buffer)
	if err != nil {
		result.Error = fmt.Errorf("failed to write file: %v", err)
		return result
	}

	result.Success = true
	result.FilePath = filePath
	return result
}

// countExistingFiles counts the number of files in the downloads directory
func countExistingFiles(downloadDir string) int {
	files, err := os.ReadDir(downloadDir)
	if err != nil {
		return 0
	}

	count := 0
	for _, file := range files {
		if !file.IsDir() {
			count++
		}
	}
	return count
}

// buildExistingFileMap creates a map of existing files for fast lookup
func buildExistingFileMap(downloadDir string) map[string]bool {
	fileMap := make(map[string]bool)
	files, err := os.ReadDir(downloadDir)
	if err != nil {
		return fileMap
	}

	for _, file := range files {
		if !file.IsDir() {
			fileMap[file.Name()] = true
		}
	}
	return fileMap
}
