/*
package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/rand"
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

// -------------------- Models & Config --------------------

type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
	JitterFactor  float64
}

var defaultRetryConfig = RetryConfig{
	MaxRetries:    3,
	InitialDelay:  1 * time.Second,
	MaxDelay:      60 * time.Second,
	BackoffFactor: 2.0,
	JitterFactor:  0.2,
}

// -------------------- User-Agent Spoofing --------------------

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) Gecko/20100101 Firefox/127.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/126.0.0.0",
}

func randomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// -------------------- Host Concurrency Limiter (FIXED) --------------------

const maxPerHost = 3

type hostLimiter struct {
	sem chan struct{}
}

var (
	hostLimiters = make(map[string]*hostLimiter)
	hostMutex    sync.RWMutex
)

func acquireHostSlot(host string) func() {
	hostMutex.RLock()
	limiter, exists := hostLimiters[host]
	hostMutex.RUnlock()

	if !exists {
		hostMutex.Lock()
		// Double-check pattern to avoid race
		limiter, exists = hostLimiters[host]
		if !exists {
			limiter = &hostLimiter{sem: make(chan struct{}, maxPerHost)}
			hostLimiters[host] = limiter
		}
		hostMutex.Unlock()
	}

	limiter.sem <- struct{}{}       // acquire
	return func() { <-limiter.sem } // release
}

// -------------------- HTTP Client --------------------

var httpClient *http.Client

func init() {
	rand.Seed(time.Now().UnixNano())

	httpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        400,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     150,
			IdleConnTimeout:     120 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

// -------------------- Utility Functions --------------------

func backoffDelay(attempt int, cfg RetryConfig) time.Duration {
	if attempt == 0 {
		return cfg.InitialDelay
	}
	d := float64(cfg.InitialDelay) * math.Pow(cfg.BackoffFactor, float64(attempt))
	jitter := d * cfg.JitterFactor * (rand.Float64()*2 - 1)
	d += jitter
	if d > float64(cfg.MaxDelay) {
		d = float64(cfg.MaxDelay)
	}
	return time.Duration(d)
}

func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	substr := []string{"timeout", "connection reset", "temporary failure", "no route", "network is unreachable"}
	for _, s := range substr {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// -------------------- Core Download Logic --------------------

func downloadOnce(urlStr string, downloadDir string) (bool, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return false, fmt.Errorf("invalid URL: %v", err)
	}

	// Acquire host slot (rate limiting)
	release := acquireHostSlot(parsed.Host)
	defer release()

	// Build request with spoofed headers
	req, _ := http.NewRequest(http.MethodGet, urlStr, nil)
	req.Header.Set("User-Agent", randomUserAgent())
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	// Execute request
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("status %d", resp.StatusCode)
	}

	// Extract filename and save
	parts := strings.Split(parsed.Path, "/")
	fname := parts[len(parts)-1]
	if fname == "" {
		fname = "unknown_file"
	}

	// Check if file already exists (skip duplicate work)
	filepath := filepath.Join(downloadDir, fname)
	if _, err := os.Stat(filepath); err == nil {
		return true, fmt.Errorf("SKIPPED_EXISTING") // special marker
	}

	file, err := os.Create(filepath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		os.Remove(filepath) // cleanup on failure
		return false, err
	}

	return true, nil
}

// -------------------- Worker & Stats --------------------

type stats struct {
	total      int64
	processed  int64
	success    int64
	failed     int64
	skipped    int64 // already existed
	downloaded int64 // actually downloaded
}

func worker(id int, jobs <-chan string, retryQueue chan<- string, downloadDir string, st *stats) {
	for urlStr := range jobs {
		success := false
		var lastErr error

		// Try with retries
		for attempt := 0; attempt <= defaultRetryConfig.MaxRetries; attempt++ {
			ok, err := downloadOnce(urlStr, downloadDir)
			lastErr = err
			if ok {
				success = true
				if err != nil && err.Error() == "SKIPPED_EXISTING" {
					atomic.AddInt64(&st.skipped, 1)
				} else {
					atomic.AddInt64(&st.downloaded, 1)
				}
				atomic.AddInt64(&st.success, 1)
				break
			}

			// Check if we should retry
			shouldRetry := retryableError(err) || (func() bool {
				if err == nil {
					return false
				}
				var sc int
				if _, parseErr := fmt.Sscanf(err.Error(), "status %d", &sc); parseErr == nil {
					return retryableStatus(sc)
				}
				return false
			})()

			if !shouldRetry || attempt == defaultRetryConfig.MaxRetries {
				break
			}

			time.Sleep(backoffDelay(attempt, defaultRetryConfig))
		}

		if !success {
			atomic.AddInt64(&st.failed, 1)
			// Log detailed failure info every 10th failure for diagnosis
			failCount := atomic.LoadInt64(&st.failed)
			if failCount <= 10 || failCount%50 == 0 {
				fmt.Printf("\nFAIL #%d: %s\n  Last error: %v\n", failCount, urlStr, lastErr)
			}
			// Queue for retry pass (non-blocking)
			select {
			case retryQueue <- urlStr:
			default:
				// If retry queue is full, just drop it
			}
		}

		atomic.AddInt64(&st.processed, 1)
	}
}

// -------------------- Progress Display --------------------

func startProgress(st *stats) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		start := time.Now()

		for range ticker.C {
			processed := atomic.LoadInt64(&st.processed)
			total := atomic.LoadInt64(&st.total)
			if total == 0 {
				continue
			}

			pct := float64(processed) / float64(total) * 100
			barWidth := 40
			filled := int(pct / 100 * float64(barWidth))
			if filled > barWidth {
				filled = barWidth
			}

			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
			elapsed := time.Since(start).Seconds()
			rate := float64(processed) / elapsed

			failed := atomic.LoadInt64(&st.failed)
			skipped := atomic.LoadInt64(&st.skipped)
			downloaded := atomic.LoadInt64(&st.downloaded)

			fmt.Printf("\r[%s] %6.2f%% %d/%d new:%d skip:%d fail:%d %.1f/s",
				bar, pct, processed, total, downloaded, skipped, failed, rate)

			if processed >= total {
				fmt.Println()
				return
			}
		}
	}()
}

// -------------------- Main Logic --------------------

func optimalConcurrency() int {
	cores := runtime.NumCPU()
	if cores >= 8 {
		return 20 // Conservative for server politeness
	}
	if cores <= 2 {
		return 5
	}
	return 10
}

func main() {
	urlFile := "aug.txt"
	if len(os.Args) > 1 {
		urlFile = os.Args[1]
	}

	downloadDir := "downloads2"
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		panic(err)
	}

	fmt.Printf("Reading URLs from: %s\n", urlFile)

	// Read URLs into slice first (simpler and more reliable)
	f, err := os.Open(urlFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(fixUnicodeEscapes(scanner.Text()))
		if line != "" && !strings.HasPrefix(line, "#") && isValidURL(line) {
			urls = append(urls, line)
		}
	}

	if len(urls) == 0 {
		fmt.Println("No valid URLs found")
		return
	}

	fmt.Printf("Found %d URLs to process\n", len(urls))

	// Setup for first pass
	st := &stats{total: int64(len(urls))}
	jobs := make(chan string, 100)
	retryQueue := make(chan string, 10000) // Large buffer to avoid blocking

	startProgress(st)

	// Start workers
	concurrency := optimalConcurrency()
	fmt.Printf("Using %d workers\n", concurrency)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id, jobs, retryQueue, downloadDir, st)
		}(i)
	}

	// Feed URLs
	go func() {
		for _, u := range urls {
			jobs <- u
		}
		close(jobs)
	}()

	wg.Wait()
	close(retryQueue)

	// Collect retry URLs
	var retryURLs []string
	for u := range retryQueue {
		retryURLs = append(retryURLs, u)
	}

	downloaded := atomic.LoadInt64(&st.downloaded)
	skipped := atomic.LoadInt64(&st.skipped)
	failed := atomic.LoadInt64(&st.failed)

	fmt.Printf("\nFirst pass complete:\n")
	fmt.Printf("  Downloaded: %d new files\n", downloaded)
	fmt.Printf("  Skipped: %d (already existed)\n", skipped)
	fmt.Printf("  Failed: %d\n", failed)
	fmt.Printf("  Total: %d\n", downloaded+skipped+failed)

	// Optional second pass for failed URLs
	if len(retryURLs) > 0 && len(retryURLs) < len(urls)/2 { // Only if less than 50% failed
		fmt.Printf("Retrying %d failed URLs after cooldown...\n", len(retryURLs))
		time.Sleep(5 * time.Minute) // Shorter cooldown

		// Reset stats for second pass
		st2 := &stats{total: int64(len(retryURLs))}
		jobs2 := make(chan string, 100)

		startProgress(st2)

		var wg2 sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg2.Add(1)
			go func(id int) {
				defer wg2.Done()
				worker(id, jobs2, make(chan string, 1000), downloadDir, st2) // Dummy retry queue
			}(i)
		}

		go func() {
			for _, u := range retryURLs {
				jobs2 <- u
			}
			close(jobs2)
		}()

		wg2.Wait()
		fmt.Printf("\nSecond pass complete. Additional success: %d\n",
			atomic.LoadInt64(&st2.success))
	}

	fmt.Println("Done!")
}

// -------------------- Helpers --------------------

func fixUnicodeEscapes(str string) string {
	str = strings.ReplaceAll(str, "\\u0026", "&")
	str = strings.ReplaceAll(str, "\\u003d", "=")
	str = strings.ReplaceAll(str, "\\u003f", "?")
	str = strings.ReplaceAll(str, "\\u007e", "~")
	return str
}

func isValidURL(str string) bool {
	return strings.HasPrefix(str, "http://") || strings.HasPrefix(str, "https://")
}
*/
