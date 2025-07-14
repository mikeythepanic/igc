package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// URLInfo represents a URL found in the JSON
type URLInfo struct {
	URL         string
	Description string
	PlanID      string
	PlanName    string
}

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
	// Create a reusable HTTP client with optimized settings
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}
}

func main() {
	fmt.Println("Starting JSON URL Scraper...")

	// Read the JSON file
	jsonFile := "../Scraper/2025-07-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index_formatted.json"
	if len(os.Args) > 1 {
		jsonFile = os.Args[1]
	}

	fmt.Printf("Reading JSON file: %s\n", jsonFile)
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Parse JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// Extract URLs
	urls := extractURLs(jsonData)
	fmt.Printf("Found %d URLs to download\n", len(urls))

	if len(urls) == 0 {
		fmt.Println("No URLs found in the JSON file")
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

	// Show initial progress
	fmt.Printf("Starting download process...\n")
	fmt.Printf("Progress: 0.0%% (0/%d)\n", len(urls))

	// Pre-check existing files in batch for faster processing
	fmt.Println("Pre-checking existing files...")
	existingFileMap := buildExistingFileMap(downloadDir)

	// Download files with higher concurrency
	results := downloadFiles(urls, downloadDir, 20, existingFileMap) // Increased from 5 to 20 concurrent downloads

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
	fmt.Printf("Files saved to: %s/\n", downloadDir)
}

// extractURLs recursively searches for URLs in the JSON data
func extractURLs(data interface{}) []URLInfo {
	var urls []URLInfo
	extractURLsRecursive(data, "", "", &urls)
	return urls
}

// extractURLsRecursive recursively searches for URLs in the JSON data
func extractURLsRecursive(data interface{}, planID, planName string, urls *[]URLInfo) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check for URL fields
		if location, ok := v["location"].(string); ok && isURL(location) {
			description := ""
			if desc, ok := v["description"].(string); ok {
				description = desc
			}
			*urls = append(*urls, URLInfo{
				URL:         location,
				Description: description,
				PlanID:      planID,
				PlanName:    planName,
			})
		}

		// Check for plan information
		if planID == "" {
			if id, ok := v["plan_id"].(string); ok {
				planID = id
			}
		}
		if planName == "" {
			if name, ok := v["plan_name"].(string); ok {
				planName = name
			}
		}

		// Recursively search all values
		for _, val := range v {
			extractURLsRecursive(val, planID, planName, urls)
		}

	case []interface{}:
		// Recursively search all elements in arrays
		for _, item := range v {
			extractURLsRecursive(item, planID, planName, urls)
		}
	}
}

// isURL checks if a string is a valid URL
func isURL(str string) bool {
	return strings.HasPrefix(str, "http://") || strings.HasPrefix(str, "https://")
}

// downloadFiles downloads multiple files concurrently
func downloadFiles(urls []URLInfo, downloadDir string, concurrency int, existingFileMap map[string]bool) []DownloadResult {
	results := make([]DownloadResult, len(urls))
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	// Progress tracking
	var completed int32
	total := len(urls)

	// Progress channel for updates
	progressChan := make(chan int, total)

	// Progress display goroutine
	go func() {
		for range progressChan {
			atomic.AddInt32(&completed, 1)
			current := atomic.LoadInt32(&completed)
			percentage := float64(current) / float64(total) * 100
			fmt.Printf("\rProgress: %.1f%% (%d/%d)", percentage, current, total)
		}
		fmt.Println() // New line after progress
	}()

	for i, urlInfo := range urls {
		wg.Add(1)
		go func(index int, info URLInfo) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			result := downloadFile(info, downloadDir, existingFileMap)
			results[index] = result

			// Send progress update
			progressChan <- 1
		}(i, urlInfo)
	}

	wg.Wait()
	close(progressChan) // Close channel to stop progress goroutine

	return results
}

// downloadFile downloads a single file
func downloadFile(urlInfo URLInfo, downloadDir string, existingFileMap map[string]bool) DownloadResult {
	result := DownloadResult{URL: urlInfo.URL}

	// Create filename from URL
	parsedURL, err := url.Parse(urlInfo.URL)
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

	// Create a more descriptive filename
	descriptiveName := filename
	if urlInfo.PlanID != "" {
		descriptiveName = urlInfo.PlanID + "_" + filename
	}
	if urlInfo.PlanName != "" {
		// Clean plan name for filename
		cleanPlanName := strings.ReplaceAll(urlInfo.PlanName, " ", "_")
		cleanPlanName = strings.ReplaceAll(cleanPlanName, "/", "_")
		cleanPlanName = strings.ReplaceAll(cleanPlanName, "\\", "_")
		descriptiveName = cleanPlanName + "_" + filename
	}

	filePath := filepath.Join(downloadDir, descriptiveName)

	// Check if file already exists using the pre-built map (much faster)
	if existingFileMap[descriptiveName] {
		result.Success = true
		result.FilePath = filePath
		return result
	}

	// Download the file using the optimized HTTP client
	resp, err := httpClient.Get(urlInfo.URL)
	if err != nil {
		result.Error = fmt.Errorf("HTTP request failed: %v", err)
		return result
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP status %d", resp.StatusCode)
		return result
	}

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to create file: %v", err)
		return result
	}
	defer file.Close()

	// Copy the response body to the file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		result.Error = fmt.Errorf("failed to write file: %v", err)
		return result
	}

	result.Success = true
	result.FilePath = filePath
	return result
}

// fileExists checks if a file exists
func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
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
