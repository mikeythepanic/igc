package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// Define the schema structure based on the actual JSON structure
type NegotiatedPrice struct {
	BillingClass   string   `json:"billing_class"`
	ExpirationDate string   `json:"expiration_date"`
	NegotiatedRate float64  `json:"negotiated_rate"`
	NegotiatedType string   `json:"negotiated_type"`
	ServiceCode    []string `json:"service_code"`
}

type NegotiatedRate struct {
	NegotiatedPrices  []NegotiatedPrice `json:"negotiated_prices"`
	ProviderReference []float64         `json:"provider_references"`
	ProviderGroups    []ProviderGroup   `json:"provider_groups"`
}

type ProviderGroup struct {
	NPI []float64 `json:"npi"`
	TIN TIN       `json:"tin"`
}

type TIN struct {
	Type  string `json:"type"`
	Value string `json:"value"`
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

// handleNullValues replaces empty or null-like strings with "N/A" for cleaner CSV output.
func handleNullValues(value string) string {
	if value == "" || value == "<nil>" || value == "null" {
		return "N/A"
	}
	return value
}

// ExtractToCSV reads a .jsonl file containing ICD10 records, flattens them, and writes them to a CSV file.
// This optimized version limits excessive columns and adds proper summary statistics.
func ExtractToCSV() {
	fmt.Println("Starting optimized CSV extraction from .jsonl file")

	// Read the JSONL file with matching objects.
	jsonlFile, err := os.Open("matches.jsonl")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("matches.jsonl not found, skipping CSV extraction.")
			return
		}
		panic(err)
	}
	defer jsonlFile.Close()

	var records []ICD10Record
	decoder := json.NewDecoder(jsonlFile)

	// Read the file stream token by token.
	for decoder.More() {
		var record ICD10Record
		if err := decoder.Decode(&record); err != nil {
			// This can happen with a malformed JSON object within the stream.
			fmt.Printf("Warning: could not decode a record: %v. Skipping object.\n", err)
			continue
		}
		records = append(records, record)
	}

	fmt.Printf("Loaded %d records from matches.jsonl\n", len(records))

	if len(records) == 0 {
		fmt.Println("No records to process")
		return
	}

	// Find reasonable maximums (limit excessive columns)
	maxServiceCodes := 0
	maxProviderRefs := 0
	maxProviderGroups := 0

	// Limit provider references to a reasonable number (e.g., 50 instead of 1798)
	const MAX_PROVIDER_REFS = 50
	const MAX_SERVICE_CODES = 100

	for _, record := range records {
		for _, rate := range record.NegotiatedRates {
			for _, price := range rate.NegotiatedPrices {
				if len(price.ServiceCode) > maxServiceCodes {
					maxServiceCodes = len(price.ServiceCode)
				}
			}
			if len(rate.ProviderReference) > maxProviderRefs {
				maxProviderRefs = len(rate.ProviderReference)
			}
			if len(rate.ProviderGroups) > maxProviderGroups {
				maxProviderGroups = len(rate.ProviderGroups)
			}
		}
	}

	// Apply reasonable limits
	if maxServiceCodes > MAX_SERVICE_CODES {
		maxServiceCodes = MAX_SERVICE_CODES
		fmt.Printf("Limiting service codes to %d columns (found %d max)\n", MAX_SERVICE_CODES, maxServiceCodes)
	}
	if maxProviderRefs > MAX_PROVIDER_REFS {
		fmt.Printf("Limiting provider references to %d columns (found %d max)\n", MAX_PROVIDER_REFS, maxProviderRefs)
		maxProviderRefs = MAX_PROVIDER_REFS
	}

	fmt.Printf("Maximum service codes per record: %d\n", maxServiceCodes)
	fmt.Printf("Maximum provider references per record: %d\n", maxProviderRefs)
	fmt.Printf("Maximum provider groups per record: %d\n", maxProviderGroups)

	// Define optimized CSV columns
	csvColumns := []string{
		"billing_code",
		"billing_code_type",
		"billing_code_type_version",
		"name",
		"negotiated_rates_count",
		"negotiation_arrangement",
		"negotiated_prices_count",
		"billing_class",
		"expiration_date",
		"negotiated_rate",
		"negotiated_type",
		"provider_references_count", // Count of provider references
		"provider_groups_count",     // Count of provider groups
		"total_npis_count",          // Total number of NPIs across all groups
		"total_tins_count",          // Total number of TINs across all groups
	}

	// Add limited service code columns
	for i := 0; i < maxServiceCodes; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("service_code_%d", i+1))
	}

	// Add limited provider reference columns
	for i := 0; i < maxProviderRefs; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("provider_reference_%d", i+1))
	}

	// Add summary columns for first provider group (instead of all individual NPIs/TINs)
	csvColumns = append(csvColumns, "first_group_npi_count")
	csvColumns = append(csvColumns, "first_group_tin_type")
	csvColumns = append(csvColumns, "first_group_tin_value")

	// Create CSV output file
	csvFile, err := os.Create("matches.csv")
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
				row[3] = record.Name
				row[4] = strconv.Itoa(len(record.NegotiatedRates))
				row[5] = record.NegotiationArrangment
				row[6] = strconv.Itoa(len(rate.NegotiatedPrices))
				row[7] = price.BillingClass
				row[8] = price.ExpirationDate
				row[9] = fmt.Sprintf("%.2f", price.NegotiatedRate)
				row[10] = price.NegotiatedType

				// Add provider and group counts (validation of counting logic)
				row[11] = strconv.Itoa(len(rate.ProviderReference)) // provider_references_count
				row[12] = strconv.Itoa(len(rate.ProviderGroups))    // provider_groups_count

				// Calculate total NPIs and TINs across all groups
				totalNPIs := 0
				totalTINs := 0
				for _, group := range rate.ProviderGroups {
					totalNPIs += len(group.NPI)
					totalTINs += 1 // Each group has exactly one TIN
				}
				row[13] = strconv.Itoa(totalNPIs) // total_npis_count
				row[14] = strconv.Itoa(totalTINs) // total_tins_count

				// Fill service code columns
				serviceCodeStart := 15
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
				providerRefStart := 15 + maxServiceCodes
				for j, providerRef := range rate.ProviderReference {
					if j < maxProviderRefs {
						row[providerRefStart+j] = strconv.FormatFloat(providerRef, 'f', -1, 64)
					}
				}
				// Fill remaining provider reference columns with empty strings
				for j := len(rate.ProviderReference); j < maxProviderRefs; j++ {
					row[providerRefStart+j] = ""
				}

				// Fill first provider group details (instead of all individual NPIs)
				firstGroupStart := 15 + maxServiceCodes + maxProviderRefs
				if len(rate.ProviderGroups) > 0 {
					firstGroup := rate.ProviderGroups[0]
					row[firstGroupStart] = strconv.Itoa(len(firstGroup.NPI))        // first_group_npi_count
					row[firstGroupStart+1] = handleNullValues(firstGroup.TIN.Type)  // first_group_tin_type
					row[firstGroupStart+2] = handleNullValues(firstGroup.TIN.Value) // first_group_tin_value
				} else {
					row[firstGroupStart] = "0"
					row[firstGroupStart+1] = ""
					row[firstGroupStart+2] = ""
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

	fmt.Printf("Extracted %d rows to matches.csv\n", rowCount)
	fmt.Println("CSV now has a manageable number of columns with proper provider/group counting validation")
}
