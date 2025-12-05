package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
POLITE SCRAPER - Designed to avoid rate limiting

Key differences from aggressive scraper:
1. Much lower concurrency (2-3 workers max)
2. Mandatory delays between requests (1-3 seconds)
3. Single connection per host at a time
4. Longer timeouts for large files
5. Exponential backoff on ANY failure
6. Saves progress to resume later
7. Randomized request timing to look human
*/

// -------------------- Progress Tracking --------------------

const progressFile = "download_progress.json"

type DownloadProgress struct {
	Completed []string `json:"completed"`
	Failed    []string `json:"failed"`
}

func loadProgress() (*DownloadProgress, map[string]bool) {
	progress := &DownloadProgress{}
	completedSet := make(map[string]bool)

	data, err := os.ReadFile(progressFile)
	if err != nil {
		return progress, completedSet
	}

	json.Unmarshal(data, progress)
	for _, url := range progress.Completed {
		completedSet[url] = true
	}
	return progress, completedSet
}

func saveProgress(progress *DownloadProgress, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()

	data, _ := json.MarshalIndent(progress, "", "  ")
	os.WriteFile(progressFile, data, 0644)
}

// -------------------- User-Agent Rotation --------------------

var userAgents = []string{
	// Chrome on Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	// Firefox on Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
	// Chrome on Mac
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	// Safari on Mac
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
	// Edge on Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

func randomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// -------------------- Polite Rate Limiting --------------------

// Global rate limiter - only ONE request at a time across all workers
var globalRateLimiter = make(chan struct{}, 1)

func init() {
	rand.Seed(time.Now().UnixNano())
	globalRateLimiter <- struct{}{} // Initialize with one token
}

// acquireGlobalSlot gets permission to make a request
// This ensures we never have more than 1 concurrent request
func acquireGlobalSlot() {
	<-globalRateLimiter
}

func releaseGlobalSlot() {
	// Add random delay before releasing (1-3 seconds)
	delay := time.Duration(1000+rand.Intn(2000)) * time.Millisecond
	time.Sleep(delay)
	globalRateLimiter <- struct{}{}
}

// -------------------- HTTP Client (Conservative) --------------------

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Timeout: 10 * time.Minute, // Very long timeout for large files
		Transport: &http.Transport{
			MaxIdleConns:          10,
			MaxIdleConnsPerHost:   2,
			MaxConnsPerHost:       2, // Only 2 connections per host
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   30 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			DisableKeepAlives:     false, // Keep connections alive
		},
	}
}

// -------------------- Download Logic --------------------

type DownloadResult struct {
	URL     string
	Success bool
	Error   error
	Size    int64
}

func downloadFile(urlStr string, downloadDir string) DownloadResult {
	result := DownloadResult{URL: urlStr}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		result.Error = fmt.Errorf("invalid URL: %v", err)
		return result
	}

	// Extract filename
	parts := strings.Split(parsed.Path, "/")
	fname := parts[len(parts)-1]
	if fname == "" {
		fname = "unknown_file"
	}

	// Check if already downloaded
	filePath := filepath.Join(downloadDir, fname)
	if info, err := os.Stat(filePath); err == nil && info.Size() > 0 {
		result.Success = true
		result.Size = info.Size()
		return result
	}

	// Acquire global rate limit slot
	acquireGlobalSlot()
	defer releaseGlobalSlot()

	// Build request with realistic headers
	req, _ := http.NewRequest(http.MethodGet, urlStr, nil)
	req.Header.Set("User-Agent", randomUserAgent())
	req.Header.Set("Accept", "application/json, application/gzip, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")
	// Add referer to look more legitimate
	req.Header.Set("Referer", fmt.Sprintf("https://%s/", parsed.Host))

	// Execute request
	resp, err := httpClient.Do(req)
	if err != nil {
		result.Error = err
		return result
	}
	defer resp.Body.Close()

	// Check status
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
		return result
	}

	// Create temp file first
	tempPath := filePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		result.Error = fmt.Errorf("create file: %v", err)
		return result
	}

	// Copy with progress tracking
	written, err := io.Copy(file, resp.Body)
	file.Close()

	if err != nil {
		os.Remove(tempPath)
		result.Error = fmt.Errorf("download: %v", err)
		return result
	}

	// Rename temp to final
	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		result.Error = fmt.Errorf("rename: %v", err)
		return result
	}

	result.Success = true
	result.Size = written
	return result
}

// -------------------- Worker with Exponential Backoff --------------------

func worker(id int, jobs <-chan string, results chan<- DownloadResult, downloadDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	consecutiveFailures := 0
	maxConsecutiveFailures := 5

	for urlStr := range jobs {
		// If we've had too many consecutive failures, back off significantly
		if consecutiveFailures >= maxConsecutiveFailures {
			backoffTime := time.Duration(consecutiveFailures*30) * time.Second
			if backoffTime > 5*time.Minute {
				backoffTime = 5 * time.Minute
			}
			fmt.Printf("\n[Worker %d] Too many failures, backing off for %v...\n", id, backoffTime)
			time.Sleep(backoffTime)
		}

		// Try up to 3 times with increasing delays
		var result DownloadResult
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				// Exponential backoff: 30s, 60s, 120s
				backoff := time.Duration(30*(1<<attempt)) * time.Second
				fmt.Printf("\n[Worker %d] Retry %d after %v...\n", id, attempt, backoff)
				time.Sleep(backoff)
			}

			result = downloadFile(urlStr, downloadDir)
			if result.Success {
				consecutiveFailures = 0
				break
			}

			// Check if error is retryable
			if result.Error != nil {
				errStr := result.Error.Error()
				// Don't retry 403/404 errors
				if strings.Contains(errStr, "403") || strings.Contains(errStr, "404") {
					break
				}
			}
			consecutiveFailures++
		}

		results <- result
	}
}

// -------------------- Stats --------------------

type Stats struct {
	total      int64
	processed  int64
	success    int64
	failed     int64
	skipped    int64
	totalBytes int64
}

func (s *Stats) Print(elapsed time.Duration) {
	processed := atomic.LoadInt64(&s.processed)
	total := atomic.LoadInt64(&s.total)
	success := atomic.LoadInt64(&s.success)
	failed := atomic.LoadInt64(&s.failed)
	skipped := atomic.LoadInt64(&s.skipped)
	bytes := atomic.LoadInt64(&s.totalBytes)

	pct := float64(processed) / float64(total) * 100
	rate := float64(processed) / elapsed.Seconds()
	mbDownloaded := float64(bytes) / (1024 * 1024)

	fmt.Printf("\r[%.1f%%] %d/%d | OK:%d Skip:%d Fail:%d | %.1f MB | %.2f/min  ",
		pct, processed, total, success, skipped, failed, mbDownloaded, rate*60)
}

// -------------------- Main --------------------

func main() {
	urlFile := "dec.txt"
	if len(os.Args) > 1 {
		urlFile = os.Args[1]
	}

	downloadDir := "downloads_polite"
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		panic(err)
	}

	fmt.Println("============================================")
	fmt.Println("  POLITE SCRAPER - Rate Limit Friendly")
	fmt.Println("============================================")
	fmt.Printf("URL file: %s\n", urlFile)
	fmt.Printf("Download dir: %s\n", downloadDir)
	fmt.Println()

	// Load progress
	progress, completedSet := loadProgress()
	fmt.Printf("Previously completed: %d URLs\n", len(completedSet))

	// Read URLs
	f, err := os.Open(urlFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Fix unicode escapes
		line = strings.ReplaceAll(line, "\\u0026", "&")
		line = strings.ReplaceAll(line, "\\u003d", "=")
		line = strings.ReplaceAll(line, "\\u003f", "?")
		line = strings.ReplaceAll(line, "\\u007e", "~")

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "http://") && !strings.HasPrefix(line, "https://") {
			continue
		}

		// Skip already completed
		if completedSet[line] {
			continue
		}

		urls = append(urls, line)
	}

	if len(urls) == 0 {
		fmt.Println("No new URLs to process")
		return
	}

	fmt.Printf("URLs to process: %d\n", len(urls))
	fmt.Println()
	fmt.Println("NOTE: This scraper is intentionally SLOW to avoid rate limiting.")
	fmt.Println("      Expected rate: ~20-40 files per minute")
	fmt.Println("      Press Ctrl+C to stop (progress is saved)")
	fmt.Println()

	// Setup channels
	jobs := make(chan string, 10)
	results := make(chan DownloadResult, 10)

	// Only 2 workers for politeness
	numWorkers := 2
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, downloadDir, &wg)
	}

	// Feed URLs in background
	go func() {
		for _, u := range urls {
			jobs <- u
		}
		close(jobs)
	}()

	// Collect results in background
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	stats := &Stats{total: int64(len(urls))}
	var progressMu sync.Mutex
	startTime := time.Now()

	// Progress ticker
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			stats.Print(time.Since(startTime))
		}
	}()

	saveCounter := 0
	for result := range results {
		atomic.AddInt64(&stats.processed, 1)

		if result.Success {
			if result.Size > 0 {
				atomic.AddInt64(&stats.success, 1)
				atomic.AddInt64(&stats.totalBytes, result.Size)
			} else {
				atomic.AddInt64(&stats.skipped, 1)
			}

			progressMu.Lock()
			progress.Completed = append(progress.Completed, result.URL)
			progressMu.Unlock()
		} else {
			atomic.AddInt64(&stats.failed, 1)

			progressMu.Lock()
			progress.Failed = append(progress.Failed, result.URL)
			progressMu.Unlock()

			// Log failures
			fmt.Printf("\n[FAIL] %s\n        %v\n", result.URL[:80]+"...", result.Error)
		}

		// Save progress every 50 files
		saveCounter++
		if saveCounter%50 == 0 {
			saveProgress(progress, &progressMu)
		}
	}

	// Final save
	saveProgress(progress, &progressMu)

	// Final stats
	fmt.Println()
	fmt.Println()
	fmt.Println("============================================")
	fmt.Println("  DOWNLOAD COMPLETE")
	fmt.Println("============================================")
	fmt.Printf("Total processed: %d\n", atomic.LoadInt64(&stats.processed))
	fmt.Printf("Successful: %d\n", atomic.LoadInt64(&stats.success))
	fmt.Printf("Skipped (existing): %d\n", atomic.LoadInt64(&stats.skipped))
	fmt.Printf("Failed: %d\n", atomic.LoadInt64(&stats.failed))
	fmt.Printf("Total downloaded: %.2f MB\n", float64(atomic.LoadInt64(&stats.totalBytes))/(1024*1024))
	fmt.Printf("Duration: %v\n", time.Since(startTime).Round(time.Second))
	fmt.Printf("Success rate: %.1f%%\n", float64(atomic.LoadInt64(&stats.success))/float64(atomic.LoadInt64(&stats.processed))*100)
}
