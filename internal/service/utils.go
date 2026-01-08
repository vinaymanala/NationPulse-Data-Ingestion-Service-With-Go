package service

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func ConstructOEDC_URL(url, indicatorCode, formerYear string) string {
	return url + "/" + indicatorCode + "?" + "startPeriod=" + formerYear + "&format=csvfilewithlabels"
}

func ExtractData(url, indicator string) []byte {
	fmt.Printf("Fetching %s data feed...\n", indicator)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error occured: %s\n", err)
		return nil
	}
	defer resp.Body.Close()
	fmt.Printf("%s Response: %s \n", indicator, resp.Status)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error occured %s\n", err)
	}
	return data
}

func GetNewTableHeaders(headers []string) []string {
	var newHeaders []string

	for _, h := range headers {
		switch h {
		case "REF_AREA":
			newHeaders = append(newHeaders, "country_code")
		case "Reference area":
			newHeaders = append(newHeaders, "country_name")
		case "MEASURE":
			newHeaders = append(newHeaders, "indicator_code")
		case "Measure":
			newHeaders = append(newHeaders, "indicator")
		case "SEX":
			newHeaders = append(newHeaders, "sex_code")
		case "Sex":
			newHeaders = append(newHeaders, "sex_name")
		case "Age":
			newHeaders = append(newHeaders, "age")
		case "TIME_PERIOD":
			newHeaders = append(newHeaders, "year")
		case "OBS_VALUE":
			newHeaders = append(newHeaders, "value")
		case "TRANSACTION":
			newHeaders = append(newHeaders, "indicator_code")
		case "Transaction":
			newHeaders = append(newHeaders, "indicator")
		case "Unit of measure":
			newHeaders = append(newHeaders, "unit_range")
		case "Casue of death":
			newHeaders = append(newHeaders, "cause")
		}
	}
	return newHeaders
}

func GetDataFeedTableHeaders(tableType string) []string {
	switch tableType {
	case "Population":
		return []string{
			"REF_AREA",
			"Reference area",
			"MEASURE",
			"Measure",
			"SEX",
			"Sex",
			"Age",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	case "Health":
		return []string{
			"REF_AREA",
			"Reference area",
			"MEASURE",
			"Measure",
			"SEX",
			"Sex",
			"Cause of death",
			"Unit of measure",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	case "EconomyGDP":
		return []string{
			"REF_AREA",
			"Reference area",
			"TRANSACTION",
			"Transaction",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	case "EconomyGov":
		return []string{
			"REF_AREA",
			"Reference area",
			"MEASURE",
			"Measure",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	case "GrowthGDP":
		return []string{
			"REF_AREA",
			"Reference area",
			"MEASURE",
			"Measure",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	case "GrowthPopulation":
		return []string{
			"REF_AREA",
			"Reference area",
			"MEASURE",
			"Measure",
			"TIME_PERIOD",
			"OBS_VALUE",
		}
	default:
	}
	return nil
}

func TransformData(data []byte, indicator string) {
	now := time.Now()
	log.Println("Started processing: ", now)
	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	if len(records) == 0 {
		log.Println("Empty records!")
		return
	}
	headersStr := GetDataFeedTableHeaders(indicator)
	// create a map of headers with key as headername and value as index
	headers := records[0]
	headerMap := make(map[string]int)
	for i, header := range headers {
		headerMap[header] = i
	}
	// fmt.Println(headerMap)
	// loop over the headers array and take the specific column index to get the position
	var headerPos []int
	for _, name := range headersStr {
		if idx, ok := headerMap[name]; ok {
			headerPos = append(headerPos, idx)
		}
	}
	// fmt.Println(headerPos)
	// loop over the records and get the records with header index only.
	var results [][]string
	for _, record := range records {
		var res []string
		for _, pos := range headerPos {
			res = append(res, record[pos])
		}
		results = append(results, res)
	}
	// change the record[0] with new headers
	results[0] = GetNewTableHeaders(headersStr)

	log.Println("Stopped processing: ", time.Since(now))
	// fmt.Println(results[:5])
	log.Println("Done Transforming data for: ", indicator)
}

func ETLDataFeed(url, indicator string) {
	byteData := ExtractData(url, indicator)
	TransformData(byteData, indicator)
}
