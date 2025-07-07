package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Define the schema structure based on your interpretation
type NegotiatedPrice struct {
	BillingClass   string   `json:"billing_class"`
	ExpirationDate string   `json:"expiration_date"`
	NegotiatedRate float64  `json:"negotiated_rate"`
	NegotiatedType string   `json:"negotiated_type"`
	ServiceCode    []string `json:"service_code"`
}

type NegotiatedRate struct {
	NegotiatedPrices  []NegotiatedPrice `json:"negotiated_prices"`
	ProviderReference []int64           `json:"provider_references"`
}

type ICD10Record struct {
	BillingCode            string           `json:"billing_code"`
	BillingCodeType        string           `json:"billing_code_type"`
	BillingCodeTypeVersion string           `json:"billing_code_type_version"`
	Description            string           `json:"description"`
	Name                   string           `json:"name"`
	NegotiatedRates        []NegotiatedRate `json:"negotiated_rates"`
	NegotiationArrangment  string           `json:"negotiation_arrangement"`
}

// Flatten nested object into dot-notation keys
func flattenObject(obj map[string]interface{}, prefix string) map[string]interface{} {
	flattened := make(map[string]interface{})

	for key, value := range obj {
		newKey := key
		if prefix != "" {
			newKey = prefix + "." + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := flattenObject(v, newKey)
			for nestedKey, nestedValue := range nested {
				flattened[nestedKey] = nestedValue
			}
		case []interface{}:
			// Handle arrays - join with pipe separator
			if len(v) > 0 {
				strValues := make([]string, len(v))
				for i, item := range v {
					strValues[i] = fmt.Sprintf("%v", item)
				}
				flattened[newKey] = strings.Join(strValues, "|")
			} else {
				flattened[newKey] = ""
			}
		default:
			flattened[newKey] = value
		}
	}

	return flattened
}

// Extract value from flattened object using dot notation
func extractValue(obj map[string]interface{}, path string) string {
	if val, ok := obj[path]; ok {
		switch v := val.(type) {
		case string:
			return v
		case float64:
			return fmt.Sprintf("%.2f", v)
		case int:
			return strconv.Itoa(v)
		case bool:
			return strconv.FormatBool(v)
		case nil:
			return ""
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// Discover all available fields in the flattened data
func discoverFields(records []map[string]interface{}) []string {
	fieldSet := make(map[string]bool)

	for _, record := range records {
		// Flatten each record first
		flattened := flattenObject(record, "")
		for field := range flattened {
			fieldSet[field] = true
		}
	}

	// Convert to slice and sort for consistent ordering
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	return fields
}

func handleNullValues(value string) string {
	if value == "" || value == "<nil>" || value == "null" {
		return "N/A"
	}
	return value
}

// Extract using the structured approach
func ExtractToCSV() {
	fmt.Println("Starting CSV extraction")

	// Read the JSON file with matching objects
	jsonFile, err := os.Open("billing_code_matches.json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()

	var records []ICD10Record
	if err := json.NewDecoder(jsonFile).Decode(&records); err != nil {
		panic(err)
	}

	fmt.Printf("Loaded %d records from billing_code_matches.json\n", len(records))

	if len(records) == 0 {
		fmt.Println("No records to process")
		return
	}

	// Find the maximum number of service codes and provider references across all records
	maxServiceCodes := 0
	maxProviderRefs := 0

	for _, record := range records {
		for _, rate := range record.NegotiatedRates {
			for _, price := range rate.NegotiatedPrices {
				if len(price.ServiceCode) > maxServiceCodes {
					maxServiceCodes = len(price.ServiceCode)
				}
				if len(rate.ProviderReference) > maxProviderRefs {
					maxProviderRefs = len(rate.ProviderReference)
				}
			}
		}
	}

	fmt.Printf("Maximum service codes per record: %d\n", maxServiceCodes)
	fmt.Printf("Maximum provider references per record: %d\n", maxProviderRefs)

	// Define CSV columns based on your schema with separate columns for arrays
	csvColumns := []string{
		"billing_code",
		"billing_code_type",
		"billing_code_type_version",
		"description",
		"name",
		"negotiated_rates_count",
		"negotiation_arrangement",
		"negotiated_prices_count",
		"billing_class",
		"expiration_date",
		"negotiated_rate",
		"negotiated_type",
	}

	// Add service code columns
	for i := 0; i < maxServiceCodes; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("service_code_%d", i+1))
	}

	// Add provider reference columns
	for i := 0; i < maxProviderRefs; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("provider_reference_%d", i+1))
	}

	// Create CSV output file
	csvFile, err := os.Create("extracted.csv")
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write header
	if err := writer.Write(csvColumns); err != nil {
		panic(err)
	}

	// Process each record
	rowCount := 0
	for i, record := range records {
		// For each negotiated rate, create a row
		for _, rate := range record.NegotiatedRates {
			// For each negotiated price, create a row
			for _, price := range rate.NegotiatedPrices {
				row := make([]string, len(csvColumns))

				// Fill basic fields
				row[0] = handleNullValues(record.BillingCode)
				row[1] = handleNullValues(record.BillingCodeType)
				row[2] = record.BillingCodeTypeVersion
				row[3] = record.Description
				row[4] = record.Name
				row[5] = strconv.Itoa(len(record.NegotiatedRates))
				row[6] = record.NegotiationArrangment
				row[7] = strconv.Itoa(len(rate.NegotiatedPrices))
				row[8] = price.BillingClass
				row[9] = price.ExpirationDate
				row[10] = fmt.Sprintf("%.2f", price.NegotiatedRate)
				row[11] = price.NegotiatedType

				// Fill service code columns
				serviceCodeStart := 12
				for j, serviceCode := range price.ServiceCode {
					if j < maxServiceCodes {
						row[serviceCodeStart+j] = handleNullValues(serviceCode)
					}
				}
				// Fill remaining service code columns with empty strings
				for j := len(price.ServiceCode); j < maxServiceCodes; j++ {
					row[serviceCodeStart+j] = ""
				}

				// Fill provider reference columns
				providerRefStart := 12 + maxServiceCodes
				for j, providerRef := range rate.ProviderReference {
					if j < maxProviderRefs {
						row[providerRefStart+j] = handleNullValues(strconv.FormatInt(providerRef, 10))
					}
				}
				// Fill remaining provider reference columns with empty strings
				for j := len(rate.ProviderReference); j < maxProviderRefs; j++ {
					row[providerRefStart+j] = ""
				}

				if err := writer.Write(row); err != nil {
					panic(err)
				}
				rowCount++
			}
		}

		if (i+1)%10 == 0 {
			fmt.Printf("Processed %d/%d records\n", i+1, len(records))
		}
	}

	fmt.Printf("Extracted %d rows to extracted.csv\n", rowCount)
}
