package jsonformatter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ParseAndFormatJSON reads a JSON file, parses it, and returns the parsed data
func ParseAndFormatJSON(inputFile string) (interface{}, error) {
	// Read JSON from input file
	input, err := os.ReadFile(inputFile)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %v", inputFile, err)
	}

	// Parse the JSON to validate it
	var data interface{}
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, fmt.Errorf("error parsing JSON in %s: %v", inputFile, err)
	}

	return data, nil
}

// FormatJSONToFile reads a JSON file, formats it, and writes to a new file
func FormatJSONToFile(inputFile string) error {
	// Read JSON from input file
	input, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("error reading file %s: %v", inputFile, err)
	}

	// Parse the JSON to validate it
	var data interface{}
	if err := json.Unmarshal(input, &data); err != nil {
		return fmt.Errorf("error parsing JSON in %s: %v", inputFile, err)
	}

	// Format the JSON with proper indentation
	formatted, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("error formatting JSON: %v", err)
	}

	// Create output filename
	baseName := strings.TrimSuffix(filepath.Base(inputFile), filepath.Ext(inputFile))
	outputFile := baseName + "_formatted.json"

	// Write formatted JSON to output file
	if err := os.WriteFile(outputFile, formatted, 0644); err != nil {
		return fmt.Errorf("error writing to %s: %v", outputFile, err)
	}

	fmt.Printf("Successfully formatted JSON from %s to %s\n", inputFile, outputFile)
	return nil
}

// GetFormattedFilename returns the formatted filename for a given input file
func GetFormattedFilename(inputFile string) string {
	baseName := strings.TrimSuffix(filepath.Base(inputFile), filepath.Ext(inputFile))
	return baseName + "_formatted.json"
}

// main function for standalone usage
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <input_file.json>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s data.json\n", os.Args[0])
		os.Exit(1)
	}

	inputFile := os.Args[1]

	if err := FormatJSONToFile(inputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
