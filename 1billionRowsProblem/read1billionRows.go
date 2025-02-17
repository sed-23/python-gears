package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CityData stores the aggregated values for each city.
type CityData struct {
	Min, Max, Avg float64
	Count         int
}

// processChunk processes a slice of lines and computes the min, max, average, and count per city.
func processChunk(chunk []string) map[string]CityData {
	localMap := make(map[string]CityData)
	for _, line := range chunk {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		city := strings.TrimSpace(parts[0])
		tmpStr := strings.TrimSpace(parts[1])
		tmp, err := strconv.ParseFloat(tmpStr, 64)
		if err != nil {
			continue
		}
		if data, ok := localMap[city]; ok {
			newCount := data.Count + 1
			// Update average using a weighted average
			data.Avg = (data.Avg*float64(data.Count) + tmp) / float64(newCount)
			if tmp < data.Min {
				data.Min = tmp
			}
			if tmp > data.Max {
				data.Max = tmp
			}
			data.Count = newCount
			localMap[city] = data
		} else {
			localMap[city] = CityData{Min: tmp, Max: tmp, Avg: tmp, Count: 1}
		}
	}
	return localMap
}

// mergeResults merges two maps of aggregated city data.
func mergeResults(map1, map2 map[string]CityData) map[string]CityData {
	for city, data2 := range map2 {
		if data1, exists := map1[city]; exists {
			totalCount := data1.Count + data2.Count
			newAvg := ((data1.Avg * float64(data1.Count)) + (data2.Avg * float64(data2.Count))) / float64(totalCount)
			mergedData := CityData{
				Min:   math.Min(data1.Min, data2.Min),
				Max:   math.Max(data1.Max, data2.Max),
				Avg:   newAvg,
				Count: totalCount,
			}
			map1[city] = mergedData
		} else {
			map1[city] = data2
		}
	}
	return map1
}

// countLines counts the total number of lines in the given file.
func countLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}

func main() {
	filename := "1billionRowInput.txt"
	chunkSize := 100000
	startTime := time.Now()

	// Count total lines to determine total chunks (for progress reporting)
	totalLines, err := countLines(filename)
	if err != nil {
		fmt.Println("Error counting lines:", err)
		return
	}
	totalChunks := int(math.Ceil(float64(totalLines) / float64(chunkSize)))

	// Open the file for processing.
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var (
		wg              sync.WaitGroup
		resultsChan     = make(chan map[string]CityData)
		mu              sync.Mutex
		finalMap        = make(map[string]CityData)
		chunksProcessed = 0
	)

	// Launch a goroutine to update progress on the same console line.
	go func() {
		for {
			mu.Lock()
			cp := chunksProcessed
			mu.Unlock()
			percentage := (float64(cp) / float64(totalChunks)) * 100
			fmt.Printf("\rProgress: %.2f%%", percentage)
			if cp >= totalChunks {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Read the file line by line and group them into chunks.
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if len(lines) >= chunkSize {
			// Copy the current chunk to avoid data race.
			chunk := make([]string, len(lines))
			copy(chunk, lines)
			wg.Add(1)
			go func(ch []string) {
				defer wg.Done()
				res := processChunk(ch)
				resultsChan <- res
			}(chunk)
			lines = nil // reset for the next chunk
		}
	}
	// Process any remaining lines as the final chunk.
	if len(lines) > 0 {
		wg.Add(1)
		chunk := make([]string, len(lines))
		copy(chunk, lines)
		go func(ch []string) {
			defer wg.Done()
			res := processChunk(ch)
			resultsChan <- res
		}(chunk)
	}

	// Close the results channel when all chunks have been processed.
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Merge the results from each processed chunk.
	for partial := range resultsChan {
		finalMap = mergeResults(finalMap, partial)
		mu.Lock()
		chunksProcessed++
		mu.Unlock()
	}

	// Ensure progress shows 100%
	fmt.Printf("\rProgress: 100.00%%\n")

	// Round the final aggregated values to 2 decimals.
	finalData := make(map[string]map[string]float64)
	for city, data := range finalMap {
		finalData[city] = map[string]float64{
			"min": math.Round(data.Min*100) / 100,
			"max": math.Round(data.Max*100) / 100,
			"avg": math.Round(data.Avg*100) / 100,
		}
	}

	fmt.Println("Final aggregated data:")
	fmt.Println(finalData)
	fmt.Printf("Processed in: %.2f seconds\n", time.Since(startTime).Seconds())
}

// Processed in: 31.46 seconds
