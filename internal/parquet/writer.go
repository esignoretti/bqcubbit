package parquet

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type Writer struct {
	props      *parquet.WriterProperties
	arrowProps pqarrow.ArrowWriterProperties
}

type WriterConfig struct {
	Compression        string
	CompressionLevel   int
	RowGroupSize       int64
	DictionaryPageSize int64
	DataPageSize       int64
}

func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		Compression:        "zstd",
		CompressionLevel:   9,
		RowGroupSize:       1024 * 1024,
		DictionaryPageSize: 2 * 1024 * 1024,
		DataPageSize:       1024 * 1024,
	}
}

func NewWriter(cfg WriterConfig) *Writer {
	codec := compress.Codecs.Zstd
	if cfg.Compression == "snappy" {
		codec = compress.Codecs.Snappy
	}

	props := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithCompressionLevel(cfg.CompressionLevel),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryPageSizeLimit(cfg.DictionaryPageSize),
		parquet.WithDataPageSize(cfg.DataPageSize),
		parquet.WithMaxRowGroupLength(cfg.RowGroupSize),
		parquet.WithStats(true),
		parquet.WithCreatedBy("bqcubbit/v0.1.0"),
		parquet.WithVersion(parquet.V2_LATEST),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)
	return &Writer{props: props, arrowProps: arrowProps}
}

func (pw *Writer) WriteStream(w io.Writer, schema *arrow.Schema, batches <-chan arrow.Record) error {
	pqWriter, err := pqarrow.NewFileWriter(schema, w, pw.props, pw.arrowProps)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	defer pqWriter.Close()

	for batch := range batches {
		if err := pqWriter.Write(batch); err != nil {
			return fmt.Errorf("write parquet batch: %w", err)
		}
		batch.Release()
	}
	return nil
}
