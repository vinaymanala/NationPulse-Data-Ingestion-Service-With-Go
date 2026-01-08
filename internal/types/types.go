package types

import (
	"context"

	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/config"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/store"
)

type Configs struct {
	Cfg config.Config
	Ctx context.Context
	DB  *store.PgClient
}
