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
// Note: This function loads all records into memory to dynamically determine the number of columns needed in the CSV.
// For extremely large datasets (many millions of records), this could be memory-intensive.
func ExtractToCSV() {
	fmt.Println("Starting CSV extraction from .jsonl file")

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

	// Find the maximum number of service codes and provider references across all records
	maxServiceCodes := 0
	maxProviderRefs := 0
	maxProviderGroups := 0
	maxNPIs := 0
	maxTINs := 0

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
			for _, group := range rate.ProviderGroups {
				if len(group.NPI) > maxNPIs {
					maxNPIs = len(group.NPI)
				}
				// TIN is now a single object, so maxTINs will be 1
				maxTINs = 1
			}
		}
	}

	fmt.Printf("Maximum service codes per record: %d\n", maxServiceCodes)
	fmt.Printf("Maximum provider references per record: %d\n", maxProviderRefs)
	fmt.Printf("Maximum provider groups per record: %d\n", maxProviderGroups)
	fmt.Printf("Maximum NPIs per group: %d\n", maxNPIs)
	fmt.Printf("Maximum TINs per group: %d\n", maxTINs)

	// Define CSV columns based on the schema
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

	// Add provider group columns
	/* for i := 0; i < maxProviderGroups; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("provider_group_%d_npi_count", i+1))
		csvColumns = append(csvColumns, fmt.Sprintf("provider_group_%d_tin_count", i+1))
	} */

	// Add NPI columns
	for i := 0; i < maxNPIs; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("npi_%d", i+1))
	}

	// Add TIN columns
	for i := 0; i < maxTINs; i++ {
		csvColumns = append(csvColumns, fmt.Sprintf("tin_%d", i+1))
	}

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
						row[providerRefStart+j] = strconv.FormatFloat(providerRef, 'f', -1, 64)
					}
				}
				// Fill remaining provider reference columns with empty strings
				for j := len(rate.ProviderReference); j < maxProviderRefs; j++ {
					row[providerRefStart+j] = ""
				}

				// Fill provider group columns (commented out)
				// providerGroupStart := 12 + maxServiceCodes + maxProviderRefs
				// for j, group := range rate.ProviderGroups {
				// 	if j < maxProviderGroups {
				// 		row[providerGroupStart+j*2] = strconv.Itoa(len(group.NPI))
				// 		row[providerGroupStart+j*2+1] = "1" // TIN is a single object
				// 	}
				// }
				// // Fill remaining provider group columns with empty strings
				// for j := len(rate.ProviderGroups); j < maxProviderGroups; j++ {
				// 	row[providerGroupStart+j*2] = ""
				// 	row[providerGroupStart+j*2+1] = ""
				// }

				// Fill NPI columns (from first provider group)
				npiStart := 12 + maxServiceCodes + maxProviderRefs
				if len(rate.ProviderGroups) > 0 {
					firstGroup := rate.ProviderGroups[0]
					for j, npi := range firstGroup.NPI {
						if j < maxNPIs {
							row[npiStart+j] = handleNullValues(strconv.FormatFloat(npi, 'f', -1, 64))
						}
					}
				}
				// Fill remaining NPI columns with empty strings
				for j := 0; j < maxNPIs; j++ {
					if j >= len(rate.ProviderGroups) || j >= len(rate.ProviderGroups[0].NPI) {
						row[npiStart+j] = ""
					}
				}

				// Fill TIN columns (from first provider group)
				tinStart := 12 + maxServiceCodes + maxProviderRefs + maxNPIs
				if len(rate.ProviderGroups) > 0 {
					firstGroup := rate.ProviderGroups[0]
					row[tinStart] = handleNullValues(firstGroup.TIN.Type)
				}
				// Fill remaining TIN columns with empty strings
				for j := 1; j < maxTINs; j++ {
					row[tinStart+j] = ""
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
}
