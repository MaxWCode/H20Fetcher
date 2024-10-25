package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// PostcodeResult holds the result for each postcode lookup
type PostcodeResult struct {
	Postcode string `json:"postcode"`
	Supplier string `json:"supplier"`
	Phone    string `json:"phone"`
	Link     string `json:"link"`
}

// AjaxResponse represents the structure of the JSON response
type AjaxResponse struct {
	Data string `json:"data"`
}

const (
	maxRetries    = 3                     // Maximum number of retries for failed requests
	maxGoroutines = 100                   // Maximum number of concurrent goroutines
	postcodeFile  = "CSV/postcodesss.csv" // CSV file containing postcodes
)

// main function
func main() {
	// Get all postcodes from the CSV file
	postcodes, err := getPostcodesFromCSV(postcodeFile)
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

	// Channel to collect results from multiple Go routines
	resultsChan := make(chan PostcodeResult, len(postcodes))

	// WaitGroup to wait for all Go routines to finish
	var wg sync.WaitGroup

	// Create a guard channel to limit concurrent goroutines
	guard := make(chan struct{}, maxGoroutines)

	// Spawn a Go routine for each postcode
	for _, postcode := range postcodes {
		wg.Add(1)
		go func(postcode string) {
			defer wg.Done()
			// Acquire a spot in the guard channel
			guard <- struct{}{}
			defer func() { <-guard }() // Release the spot when done

			// Perform the request with retries and get the supplier information
			result := getSupplierForPostcodeWithRetries(postcode, maxRetries)
			resultsChan <- result
		}(postcode)
	}

	// Wait for all Go routines to finish and close the results channel
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect all results into a slice
	var results []PostcodeResult
	for result := range resultsChan {
		results = append(results, result)
	}

	// Save results to a JSON file
	saveResultsToJSON(results, "water_suppliers_results.json")
}

// getPostcodesFromCSV reads a single CSV file and extracts postcodes
func getPostcodesFromCSV(filePath string) ([]string, error) {
	var postcodes []string

	// Open the CSV file
	csvFile, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)

	// Read each row of the CSV
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV file: %v", err)
		}

		// Add the postcode from the first column (assuming postcodes are in the first column)
		postcodes = append(postcodes, record[0])
	}

	return postcodes, nil
}

// saveResultsToJSON saves the results slice into a JSON file
func saveResultsToJSON(results []PostcodeResult, filename string) {
	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("Error marshalling results to JSON: %v", err)
	}

	// Write JSON data to a file
	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		log.Fatalf("Error writing to JSON file: %v", err)
	}

	fmt.Printf("Results saved to %s\n", filename)
}

// getSupplierForPostcodeWithRetries performs the POST request with retries
func getSupplierForPostcodeWithRetries(postcode string, retries int) PostcodeResult {
	var result PostcodeResult

	for i := 0; i < retries; i++ {
		result = getSupplierForPostcode(postcode)

		// Check if the supplier was found
		if result.Supplier != "Not Found" {
			fmt.Printf("[Postcode %s] Successful result on attempt %d: %s\n", postcode, i+1, result.Supplier)
			return result
		}

		// Log the attempt and result
		fmt.Printf("[Postcode %s] Attempt %d: Extracted supplier: %s\n", postcode, i+1, result.Supplier)

		// Wait before retrying
		time.Sleep(2 * time.Second) // Optional: Wait before retrying
	}

	fmt.Printf("[Postcode %s] All attempts failed. Last result: %s\n", postcode, result.Supplier)
	return result // Return the result after all retries
}

// getSupplierForPostcode performs the POST request to get the supplier info for a given postcode
func getSupplierForPostcode(postcode string) PostcodeResult {
	endpointURL := "https://www.water.org.uk/customers/find-your-supplier?ajax_form=1&_wrapper_format=drupal_ajax"

	// Data payload for the POST request
	formData := url.Values{
		"postcode":                  {postcode},
		"form_build_id":             {"form-L5pD8ZkLBHXVZ8bFpzrd3oIEPn94DYlRz298X2_IG1s"}, // Adjust as necessary
		"form_id":                   {"wateruk_find_my_supplier"},
		"_triggering_element_name":  {"op"},
		"_triggering_element_value": {"Submit"},
		"_drupal_ajax":              {"1"},
	}

	fmt.Printf("[Postcode %s] Sending request...\n", postcode)

	// Create the POST request
	req, err := http.NewRequest("POST", endpointURL, strings.NewReader(formData.Encode()))
	if err != nil {
		fmt.Printf("Error creating request for postcode %s: %v\n", postcode, err)
		return PostcodeResult{Postcode: postcode}
	}

	// Set minimal headers
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("User-Agent", "Mozilla/5.0")

	// Perform the POST request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request for postcode %s: %v\n", postcode, err)
		return PostcodeResult{Postcode: postcode}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Received non-OK HTTP status for postcode %s: %s\n", postcode, resp.Status)
		return PostcodeResult{Postcode: postcode}
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response for postcode %s: %v\n", postcode, err)
		return PostcodeResult{Postcode: postcode}
	}

	// Parse the JSON response
	var ajaxResponse []AjaxResponse
	if err := json.Unmarshal(body, &ajaxResponse); err != nil {
		fmt.Printf("Error parsing JSON response for postcode %s: %v\n", postcode, err)
		return PostcodeResult{Postcode: postcode}
	}

	// Extract supplier details from the HTML in the data field
	supplier := extractSupplierDetails(ajaxResponse[2].Data) // Assuming the third item contains the required HTML
	fmt.Printf("[Postcode %s] Extracted Results: %s...\n", postcode, supplier["link"])
	return PostcodeResult{
		Postcode: postcode,
		Supplier: supplier["name"],
		Phone:    supplier["phone"],
		Link:     supplier["link"],
	}
}

// extractSupplierDetails extracts the supplier name, phone, and link from the HTML response
func extractSupplierDetails(body string) map[string]string {
	details := make(map[string]string)

	// Regular expressions to extract the supplier name, phone, and link
	reName := regexp.MustCompile(`<h2 class="supplier__name">(.+?)</h2>`)
	rePhone := regexp.MustCompile(`<p class="supplier__phone">General enquiries call <b>(.+?)</b></p>`)
	reLink := regexp.MustCompile(`<a class="supplier__link.+?href="(.+?)".*?>`)

	// Find matches
	nameMatch := reName.FindStringSubmatch(body)
	phoneMatch := rePhone.FindStringSubmatch(body)
	linkMatch := reLink.FindStringSubmatch(body)

	// Extracted details
	if len(nameMatch) > 1 {
		details["name"] = nameMatch[1]
	} else {
		details["name"] = "Not Found"
	}

	if len(phoneMatch) > 1 {
		details["phone"] = phoneMatch[1]
	} else {
		details["phone"] = "Not Found"
	}

	if len(linkMatch) > 1 {
		details["link"] = linkMatch[1]
	} else {
		details["link"] = "Not Found"
	}

	return details
}
