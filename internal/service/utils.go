package service

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/types"
)

func ConstructOEDC_URL(url, indicatorCode, formerYear string) string {
	return url + "/" + indicatorCode + "?" + "startPeriod=" + formerYear + "&format=csvfilewithlabels"
}

func ExtractData(url string, indicator string) ([]byte, error) {
	log.Printf("Fetching %s data feed...\n", indicator)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error occured: %s\n", err)
		return nil, err
	}
	defer resp.Body.Close()
	log.Printf("%s Response: %s \n", indicator, resp.Status)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error occured %s\n", err)
		return nil, err
	}
	return data, nil
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
		case "Cause of death":
			newHeaders = append(newHeaders, "cause")
		}
	}
	return newHeaders
}

func GetDataFeedTableHeaders(tableType string) []string {
	switch tableType {
	case "Population":
		return PopulationTableColNames
	case "Health":
		return HealthTableColNames
	case "EconomyGDP":
		return EconomyGDPTableColNames
	case "EconomyGov":
		return EcnomyGovTableColNames
	case "GrowthGDP":
		return GrowthGDPTableColNames
	case "GrowthPopulation":
		return GrowthPopulationTableColNames
	default:
	}
	return nil
}

func TransformData(data []byte, indicator string) ([][]string, error) {
	now := time.Now()
	log.Println("Started processing data: ", now)
	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	if len(records) == 0 {
		log.Println("Empty records!")
		return nil, err
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

	return results, nil
}

func ExecuteQueries(c *types.Configs, ctx context.Context, results [][]string, indicator string) error {
	var insertTableSqlStatement string
	var tableName string
	var createTableSqlStatement string
	switch indicator {
	case "Population":
		tableName = PopulationTableName
		createTableSqlStatement = PopulationTableCreateSqlStatement
		insertTableSqlStatement = PopulationTableInsertSqlStatement
	case "Health":
		tableName = HealthTableName
		createTableSqlStatement = HealthTableCreateSqlStatement
		insertTableSqlStatement = HealthTableInsertSqlStatment
	case "EconomyGDP":
		tableName = EconomyGDPTableName
		createTableSqlStatement = EconomyGDPTableCreateSqlStatement
		insertTableSqlStatement = EconomyGDPTableInsertSqlStatment
	case "EconomyGov":
		tableName = EconomyGovTableName
		createTableSqlStatement = EconomyGovTableCreateSqlStatement
		insertTableSqlStatement = EconomyGovTableInsertSqlStatement
	case "GrowthGDP":
		tableName = GrowthGDPTableName
		createTableSqlStatement = GrowthGdpTableCreateSqlStatement
		insertTableSqlStatement = GrowthGDPTableInsertSqlStatement
	case "GrowthPopulation":
		tableName = GrowthPopulationTableName
		createTableSqlStatement = GrowthPopulationCreateSqlStatement
		insertTableSqlStatement = GrowthPopulationTableInsertSqlStatement
	}

	dropCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Drop table if exists
	dropTableSqlStatement := `DROP TABLE IF EXISTS ` + tableName + ` CASCADE`
	log.Println("DROP Query:", dropTableSqlStatement)
	d, err := c.DB.Client.Query(dropCtx, dropTableSqlStatement)
	if err != nil {
		log.Println("Drop table failed: ", err)
		return err
	}
	d.Close()
	log.Println("TABLE ", tableName, " dropped (if it existed)")

	createCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Create a new table
	e, err := c.DB.Client.Exec(createCtx, createTableSqlStatement, tableName)
	if err != nil {
		log.Println("Error create table: ", err)
		return err
	}
	if e.RowsAffected() == 0 {
		log.Println("Created table ", tableName, " successfully.")
	}

	// Insert new records
	headersStr := GetDataFeedTableHeaders(indicator)
	headers := GetNewTableHeaders(headersStr)
	log.Println("Inserting data...")

	const BATCH_SIZE = 500
	for i := 1; i < len(results); i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if len(results) < end {
			end = len(results)
		}
		batch := &pgx.Batch{}
		// log.Println("Create batch..")
		for k := i; k < end; k++ {
			record := results[k]
			var args []any
			args = append(args, tableName)
			for pos := range headers {
				if pos < len(record) {
					if record[pos] == "" && pos == len(headers)-1 {
						args = append(args, "0")
					} else {
						args = append(args, record[pos])
					}
				} else {
					args = append(args, "0")
				}
			}

			batch.Queue(insertTableSqlStatement, args...)
		}
		// log.Printf("Queue completed with %d queries. Sending batch to insert into table...\n", batch.Len())
		// Add timeout context
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := executeBatchWithRetry(batchCtx, c, batch)
		cancel()

		if err != nil {
			log.Printf("Error executing batch: %v", err)
			return err
		}
	}
	log.Println("Insert table ", tableName, " done successfully.")
	return nil
}

func executeBatchWithRetry(ctx context.Context, c *types.Configs, batch *pgx.Batch) error {
	maxRetries := 3
	backoff := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		batchResults := c.DB.Client.SendBatch(ctx, batch)

		hasError := false
		for i := 0; i < batch.Len(); i++ {
			_, err := batchResults.Exec()
			if err != nil {
				log.Printf("Error executing batch query %d (attempt %d): %v", i, attempt+1, err)
				hasError = true
				break
			}
		}

		if err := batchResults.Close(); err != nil {
			log.Fatalf("Error closing batch results: %v\n", err)
		}

		if !hasError {
			return nil
		}

		if attempt < maxRetries-1 {
			log.Printf("Retrying batch execution in %v..", backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return errors.New("Batch execution failed after retries" + strconv.Itoa(maxRetries))
}
func LoadData(c *types.Configs, ctx context.Context, results [][]string, indicator string) error {
	err := ExecuteQueries(c, ctx, results, indicator)
	return err
}
func ETLDataFeed(c *types.Configs, ctx context.Context, url, indicator string, errCh chan<- struct{}) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	byteData, err := ExtractData(url, indicator)
	if err != nil {
		errCh <- struct{}{}
		return
	}
	results, err := TransformData(byteData, indicator)
	if err != nil {
		errCh <- struct{}{}
		return
	}
	if err := LoadData(c, ctx, results, indicator); err != nil {
		errCh <- struct{}{}
		return
	}
	log.Printf("Process %s completed successfully\n", indicator)
}
