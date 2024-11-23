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
	"path/filepath"
	"regexp"
	"sort"
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

// Progress tracks the current state of processing
type Progress struct {
	LastFile     string `json:"last_file"`     // Last CSV file processed
	LastPostcode string `json:"last_postcode"` // Last postcode processed
	Completed    bool   `json:"completed"`     // Whether all processing is complete
}

const (
	maxRetries    = 3
	maxGoroutines = 3
	postcodeDir   = "ALLCODECSV"
	progressFile  = "progress.json"
	resultsFile   = "water_suppliers_results.json"
)

// loadProgress loads the current progress from the progress file
func loadProgress() (*Progress, error) {
	data, err := os.ReadFile(progressFile)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist, return new progress
			return &Progress{}, nil
		}
		return nil, fmt.Errorf("error reading progress file: %v", err)
	}

	var progress Progress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, fmt.Errorf("error parsing progress file: %v", err)
	}

	return &progress, nil
}

// saveProgress saves the current progress to the progress file
func saveProgress(progress *Progress) error {
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling progress: %v", err)
	}

	if err := os.WriteFile(progressFile, data, 0644); err != nil {
		return fmt.Errorf("error writing progress file: %v", err)
	}

	return nil
}

// loadExistingResults loads any existing results from the results file
func loadExistingResults() ([]PostcodeResult, error) {
	data, err := os.ReadFile(resultsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []PostcodeResult{}, nil
		}
		return nil, fmt.Errorf("error reading results file: %v", err)
	}

	var results []PostcodeResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("error parsing results file: %v", err)
	}

	return results, nil
}

func main() {
	// Load progress from previous run
	progress, err := loadProgress()
	if err != nil {
		log.Fatalf("Error loading progress: %v", err)
	}

	// Load any existing results
	existingResults, err := loadExistingResults()
	if err != nil {
		log.Fatalf("Error loading existing results: %v", err)
	}

	// Create a map of processed postcodes for quick lookup
	processedPostcodes := make(map[string]bool)
	for _, result := range existingResults {
		processedPostcodes[result.Postcode] = true
	}

	// Get list of CSV files
	files, err := filepath.Glob(filepath.Join(postcodeDir, "*.csv"))
	if err != nil {
		log.Fatalf("Error reading directory: %v", err)
	}

	// Sort files to ensure consistent ordering
	sort.Strings(files)

	// Find starting point based on progress
	startIdx := 0
	if progress.LastFile != "" {
		for i, file := range files {
			if filepath.Base(file) == progress.LastFile {
				startIdx = i
				break
			}
		}
	}

	// Process each file from the last known position
	var results []PostcodeResult
	results = append(results, existingResults...)

	for i := startIdx; i < len(files); i++ {
		file := files[i]
		filename := filepath.Base(file)
		log.Printf("Processing file: %s", filename)

		postcodes, err := getPostcodesFromCSV(file)
		if err != nil {
			log.Printf("Error reading CSV file %s: %v", file, err)
			continue
		}

		// Find starting postcode in current file
		startPostcodeIdx := 0
		if filename == progress.LastFile && progress.LastPostcode != "" {
			for j, pc := range postcodes {
				if pc == progress.LastPostcode {
					startPostcodeIdx = j + 1 // Start from the NEXT postcode
					if startPostcodeIdx < len(postcodes) {
						log.Printf("Resuming from postcode %s (after %s)", postcodes[startPostcodeIdx], pc)
					}
					break
				}
			}
		}

		// Create channels for concurrent processing
		resultsChan := make(chan PostcodeResult, maxGoroutines)
		errorsChan := make(chan error, maxGoroutines)
		semaphore := make(chan struct{}, maxGoroutines)
		var wg sync.WaitGroup

		// Process postcodes with concurrent workers
		for j := startPostcodeIdx; j < len(postcodes); j++ {
			postcode := postcodes[j]

			// Skip if already processed
			if processedPostcodes[postcode] {
				log.Printf("Skipping already processed postcode: %s", postcode)
				continue
			}

			wg.Add(1)
			semaphore <- struct{}{} // Acquire semaphore

			go func(pc string, idx int) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				result := getSupplierForPostcodeWithRetries(pc, maxRetries)
				resultsChan <- result

				// Update progress
				if idx > startPostcodeIdx {
					progress.LastFile = filename
					progress.LastPostcode = pc
					if err := saveProgress(progress); err != nil {
						errorsChan <- fmt.Errorf("error saving progress for postcode %s: %v", pc, err)
					}
				}
			}(postcode, j)

			// Wait for all goroutines to complete before moving to next batch
			if j%maxGoroutines == maxGoroutines-1 || j == len(postcodes)-1 {
				go func() {
					wg.Wait()
					close(resultsChan)
				}()

				// Collect results
				for result := range resultsChan {
					if result.Supplier != "" && result.Supplier != "Not Found" {
						processedPostcodes[result.Postcode] = true
						results = append(results, result)
					}
				}

				// Save results periodically
				if len(results)%10 == 0 {
					saveResultsToJSON(results, resultsFile)
				}

				// Check for errors
				select {
				case err := <-errorsChan:
					log.Printf("Error during processing: %v", err)
				default:
				}

				// Reset channels for next batch
				resultsChan = make(chan PostcodeResult, maxGoroutines)
				errorsChan = make(chan error, maxGoroutines)
			}
		}

		// Save results after completing each file
		saveResultsToJSON(results, resultsFile)

		// If we've completed a file, clear the last postcode
		if i < len(files)-1 {
			progress.LastPostcode = ""
			if err := saveProgress(progress); err != nil {
				log.Printf("Error saving progress: %v", err)
			}
		}
	}

	// Mark as completed
	progress.Completed = true
	if err := saveProgress(progress); err != nil {
		log.Printf("Error saving final progress: %v", err)
	}

	log.Println("Processing completed successfully")
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

		// Extract postcode from the first column and remove quotes if present
		postcode := strings.Trim(record[0], "\"")
		postcodes = append(postcodes, postcode)
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
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("[Postcode %s] All attempts failed. Last result: %s\n", postcode, result.Supplier)
	return result
}

// getSupplierForPostcode performs the POST request to get the supplier info for a given postcode
func getSupplierForPostcode(postcode string) PostcodeResult {
	endpointURL := "https://www.water.org.uk/customers/find-your-supplier?ajax_form=1&_wrapper_format=drupal_ajax"

	// Data payload for the POST request
	formData := url.Values{
		"postcode":                  {postcode},
		"form_build_id":             {"form-L5pD8ZkLBHXVZ8bFpzrd3oIEPn94DYlRz298X2_IG1s"},
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
	supplier := extractSupplierDetails(ajaxResponse[2].Data)
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
