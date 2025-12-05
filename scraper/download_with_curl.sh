#!/bin/bash

# Simple curl-based downloader for URL lists
# Usage: ./download_with_curl.sh [url_file] [download_dir]

set -e

# Default values
URL_FILE="${1:-aug.txt}"
DOWNLOAD_DIR="${2:-downloads_curl}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== cURL Sequential Downloader ===${NC}"
echo -e "URL file: ${GREEN}$URL_FILE${NC}"
echo -e "Download directory: ${GREEN}$DOWNLOAD_DIR${NC}"
echo ""

# Check if URL file exists
if [[ ! -f "$URL_FILE" ]]; then
    echo -e "${RED}Error: URL file '$URL_FILE' not found${NC}"
    exit 1
fi

# Create download directory
mkdir -p "$DOWNLOAD_DIR"

# Function to fix unicode escapes in URLs
fix_unicode_escapes() {
    local url="$1"
    # Fix common unicode escapes
    url="${url//\\u0026/&}"    # &
    url="${url//\\u003d/=}"    # =
    url="${url//\\u003f/?}"    # ?
    url="${url//\\u007e/~}"    # ~
    url="${url//\\u002f//}"    # /
    url="${url//\\u003a/:}"    # :
    url="${url//\\u005f/_}"    # _
    url="${url//\\u002d/-}"    # -
    url="${url//\\u002e/.}"    # .
    echo "$url"
}

# Function to download a single file
download_file() {
    local url="$1"
    local fixed_url
    local filename
    local filepath
    
    # Fix unicode escapes
    fixed_url=$(fix_unicode_escapes "$url")
    
    # Extract filename from URL (get the path part before query parameters)
    url_path=$(echo "$fixed_url" | cut -d'?' -f1)
    filename=$(basename "$url_path")
    if [[ -z "$filename" || "$filename" == "." || "$filename" == "/" ]]; then
        filename="unknown_file_$(date +%s%N)"
    fi
    
    filepath="$DOWNLOAD_DIR/$filename"
    
    # Skip if file already exists
    if [[ -f "$filepath" ]]; then
        echo -e "${YELLOW}SKIP${NC} $filename (already exists)"
        return 0
    fi
    
    # Download with curl
    if curl -L -s -S --fail \
        -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36" \
        -H "Accept: */*" \
        -H "Accept-Language: en-US,en;q=0.9" \
        --connect-timeout 30 \
        --max-time 300 \
        --retry 3 \
        --retry-delay 1 \
        --retry-max-time 60 \
        -o "$filepath" \
        "$fixed_url"; then
        echo -e "${GREEN}OK${NC}   $filename"
        return 0
    else
        local exit_code=$?
        echo -e "${RED}FAIL${NC} $filename (curl exit code: $exit_code)"
        echo -e "  ${RED}URL:${NC} $fixed_url"
        # Remove partial file on failure
        [[ -f "$filepath" ]] && rm -f "$filepath"
        return 1
    fi
}

# No exports needed for sequential execution

# Count total URLs
total_urls=$(grep -v '^#\|^$' "$URL_FILE" | wc -l)
echo -e "Processing ${GREEN}$total_urls${NC} URLs..."
echo ""

# Create temporary file with valid URLs only
temp_urls=$(mktemp)
trap "rm -f $temp_urls" EXIT

# Filter valid URLs (skip comments and empty lines)
grep -v '^#\|^$' "$URL_FILE" | while read -r url; do
    if [[ "$url" =~ ^https?:// ]]; then
        echo "$url"
    fi
done > "$temp_urls"

# Start timer
start_time=$(date +%s)

# Download files sequentially one by one
echo "Starting sequential downloads..."
echo "First few URLs:"
head -3 "$temp_urls" | while read -r url; do
    fixed=$(fix_unicode_escapes "$url")
    echo -e "  ${BLUE}Original:${NC} $url"
    echo -e "  ${GREEN}Fixed:${NC}    $fixed"
    echo ""
done

current=0
while read -r url; do
    current=$((current + 1))
    echo -e "${BLUE}[$current/$total_urls]${NC} Processing..."
    download_file "$url"
done < "$temp_urls"

# Calculate stats
end_time=$(date +%s)
duration=$((end_time - start_time))
downloaded_count=$(find "$DOWNLOAD_DIR" -type f | wc -l)

echo ""
echo -e "${BLUE}=== Download Complete ===${NC}"
echo -e "Files downloaded: ${GREEN}$downloaded_count${NC}"
echo -e "Total time: ${GREEN}${duration}s${NC}"
echo -e "Download directory: ${GREEN}$DOWNLOAD_DIR${NC}"

# Show some stats about file sizes
if [[ $downloaded_count -gt 0 ]]; then
    total_size=$(du -sh "$DOWNLOAD_DIR" 2>/dev/null | cut -f1)
    echo -e "Total size: ${GREEN}$total_size${NC}"
fi

echo ""
echo -e "${GREEN}Done!${NC}"
