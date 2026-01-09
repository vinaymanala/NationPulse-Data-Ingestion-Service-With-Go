package service

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/types"
)

var (
	CURRENT_YEAR = time.Now().Year()
	FORMER_YEAR  = CURRENT_YEAR - 16
)

type DataIngestionSvc struct {
	configs *types.Configs
}

func NewDataIngestionSvc(configs *types.Configs) *DataIngestionSvc {
	return &DataIngestionSvc{
		configs: configs,
	}
}

func (d *DataIngestionSvc) Initialize() {
	log.Println("Cleaning up idle connections...")
	_, err := d.configs.DB.Client.Exec(d.configs.Ctx, `
		SELECT pg_terminate_backend(pid) 
		FROM pg_stat_activity 
		WHERE usename = 'postgres' 
		  AND pid <> pg_backend_pid();
	`)
	if err != nil {
		log.Printf("Warning: Could not kill idle connections: %v\n", err)
	}
	log.Println("Closed connections successfully..")
	time.Sleep(1 * time.Second)
}

func (d *DataIngestionSvc) Serve() {
	var wg sync.WaitGroup
	now := time.Now()
	indicators := map[string]string{
		"Population":       d.configs.Cfg.POPULATION_INDICATOR,
		"Health":           d.configs.Cfg.HEALTH_INDICATOR,
		"EconomyGDP":       d.configs.Cfg.ECONOMY_GDP_INDICATOR,
		"EconomyGov":       d.configs.Cfg.ECONOMY_GOV_INDICATOR,
		"GrowthGDP":        d.configs.Cfg.GROWTH_GDP_INDICATOR,
		"GrowthPopulation": d.configs.Cfg.GROWTH_POPULATION_INDICATOR,
	}

	for indicator, indicatorCode := range indicators {
		formattedUrl := ConstructOEDC_URL(d.configs.Cfg.BASE_URL, indicatorCode, strconv.Itoa(FORMER_YEAR))
		fmt.Println("URL constructed", formattedUrl)

		wg.Go(func() {
			ETLDataFeed(d.configs, formattedUrl, indicator)
		})

	}
	wg.Wait()

	log.Println("Time taken to finish the job: ", time.Since(now))

}
