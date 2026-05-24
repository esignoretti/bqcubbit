package sync

import (
	"context"
	"log"

	"github.com/esignoretti/bqcubbit/internal/bigquery"
	"github.com/esignoretti/bqcubbit/internal/config"
	"github.com/esignoretti/bqcubbit/internal/parquet"
	"github.com/esignoretti/bqcubbit/internal/rate"
	"github.com/esignoretti/bqcubbit/internal/storage"
)

type TaskExecutor struct {
	cfg       *config.Config
	bqStorage *bigquery.StorageReadReader
	bqExport  *bigquery.ExportDataBackend
	storage   *storage.Client
	pqWriter  *parquet.Writer
	limiters  *rate.Limiters
}

func NewTaskExecutor(
	cfg *config.Config,
	bqStorage *bigquery.StorageReadReader,
	bqExport *bigquery.ExportDataBackend,
	storage *storage.Client,
	pqWriter *parquet.Writer,
	limiters *rate.Limiters,
) *TaskExecutor {
	return &TaskExecutor{
		cfg: cfg, bqStorage: bqStorage, bqExport: bqExport,
		storage: storage, pqWriter: pqWriter, limiters: limiters,
	}
}

func (e *TaskExecutor) ExecuteTask(ctx context.Context, taskID string) error {
	log.Printf("[executor] executing task %s", taskID)
	return nil
}
