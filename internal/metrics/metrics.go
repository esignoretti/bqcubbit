package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	BytesExtracted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bqcubbit_bytes_extracted_total",
		Help: "Total bytes read from BigQuery",
	}, []string{"dataset", "table"})

	BytesUploaded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bqcubbit_bytes_uploaded_total",
		Help: "Total bytes uploaded to Cubbit",
	}, []string{"dataset", "table"})

	CompressionRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bqcubbit_compression_ratio",
		Help: "Compression ratio (extracted / stored)",
	}, []string{"dataset", "table"})

	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bqcubbit_task_duration_seconds",
		Help:    "Task execution duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset", "table", "status"})

	TasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bqcubbit_tasks_total",
		Help: "Total tasks processed",
	}, []string{"status"})

	PartitionLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bqcubbit_partition_lag_seconds",
		Help: "Time since last successful sync per partition",
	}, []string{"dataset", "table", "partition"})
)

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
