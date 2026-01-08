package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/config"
)

type PgClient struct {
	Client *pgxpool.Pool
}

var (
	pgInstance *PgClient
	pgOnce     sync.Once
)

func NewPgClient(ctx context.Context, cfg config.Config) *PgClient {
	// pgHost := cfg.PostgresHost
	pgName := cfg.PostgresName
	pgPass := cfg.PostgresPass
	pgUser := cfg.PostgresUser
	pgAddr := cfg.PostgresAddr
	// connStr := "postgres://postgres:postgres@localhost:5432/nationPulseDB?sslmode=disable"
	connStr := "postgres://" + pgUser + ":" + pgPass + "@" + pgAddr + "/" + pgName + "?sslmode=disable"
	fmt.Println(connStr)
	pgOnce.Do(func() {
		db, err := pgxpool.New(ctx, connStr)
		if err != nil {
			fmt.Printf("Error occured while connecting database: %s\n", err)
			panic(err)
		}
		pgInstance = &PgClient{Client: db}
	})
	fmt.Println("Connected to Postgres database successfully")
	return pgInstance
}

func (pg *PgClient) Ping(ctx context.Context) error {
	return pg.Client.Ping(ctx)
}

// func (pg *PgClient) FetchFromDB(ctx context.Context, req ExportRequest) ([][]string, error) {

// 	query := req.Filters.Query
// 	countryCode := req.RequestCountryCode
// 	rows, err := pg.Client.Query(ctx, query, countryCode)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	data, err := pgx.CollectRows(rows, pgx.RowToFunc[[]string](func(row pgx.CollectableRow) ([]string, error) {
// 		values, err := row.Values()
// 		if err != nil {
// 			return nil, err
// 		}

// 		var rowData []string
// 		for _, val := range values {
// 			if val != nil {
// 				rowData = append(rowData, fmt.Sprintf("%v", val))
// 			} else {
// 				rowData = append(rowData, fmt.Sprintf("%v", val))
// 			}
// 		}

// 		return rowData, nil
// 	}))
// 	if err != nil {
// 		log.Fatalf("Error scanning a row: %v\n", err)
// 		return nil, err
// 	}

// 	return data, nil
// }
