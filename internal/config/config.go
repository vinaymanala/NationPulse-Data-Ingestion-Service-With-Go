package config

import (
	"os"
	"strconv"
)

type Config struct {
	// Port
	Port int
	//Postgres
	PostgresHost string
	PostgresPass string
	PostgresName string
	PostgresUser string
	PostgresAddr string
	//Data ingestion config
	BASE_URL                    string
	DELAY_BETWEEN_REQUESTS      int
	POPULATION_INDICATOR        string
	ECONOMY_GDP_INDICATOR       string
	ECONOMY_GOV_INDICATOR       string
	HEALTH_INDICATOR            string
	GROWTH_GDP_INDICATOR        string
	GROWTH_POPULATION_INDICATOR string
}

func Load() Config {
	defaultPort := 8080

	portStr := os.Getenv("PORT")
	if portStr == "" {
		portStr = "8080"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = defaultPort
	}

	pgHost := os.Getenv("PG_DB_HOST")
	if pgHost == "" {
		pgHost = "postgres-db"
	}

	pgName := os.Getenv("PG_DB_NAME")
	if pgName == "" {
		pgName = "nationPulseDB"
	}

	pgUser := os.Getenv("PG_DB_USER")
	if pgUser == "" {
		pgUser = "postgres"
	}

	pgPass := os.Getenv("PG_DB_PASS")
	if pgPass == "" {
		pgPass = "postgres"
	}

	pgAddr := os.Getenv("PG_DB_ADDR")
	if pgAddr == "" {
		pgAddr = "localhost:5432"
	}

	delay_request, err := strconv.Atoi(os.Getenv("DELAY_BETWEEN_REQUESTS"))
	if err != nil {
		delay_request = 1
	}
	return Config{
		Port:                        port,
		PostgresHost:                pgHost,
		PostgresName:                pgName,
		PostgresUser:                pgUser,
		PostgresPass:                pgPass,
		PostgresAddr:                pgAddr,
		BASE_URL:                    os.Getenv("BASE_URL"),
		DELAY_BETWEEN_REQUESTS:      delay_request,
		POPULATION_INDICATOR:        os.Getenv("POPULATION_INDICATOR"),
		HEALTH_INDICATOR:            os.Getenv("HEALTH_INDICATOR"),
		ECONOMY_GDP_INDICATOR:       os.Getenv("ECONOMY_GDP_INDICATOR"),
		ECONOMY_GOV_INDICATOR:       os.Getenv("ECONOMY_GOV_INDICATOR"),
		GROWTH_GDP_INDICATOR:        os.Getenv("GROWTH_GDP_INDICATOR"),
		GROWTH_POPULATION_INDICATOR: os.Getenv("GROWTH_POPULATION_INDICATOR"),
	}
}
