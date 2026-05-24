package parquet

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestWriteStream(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	batches := make(chan arrow.Record, 2)
	go func() {
		defer close(batches)
		for i := int64(0); i < 100; i += 10 {
			ids := make([]int64, 10)
			names := make([]string, 10)
			for j := 0; j < 10; j++ {
				ids[j] = i + int64(j)
				names[j] = "name"
			}

			idBldr := array.NewInt64Builder(pool)
			idBldr.AppendValues(ids, nil)
			idCol := idBldr.NewInt64Array()

			nameBldr := array.NewStringBuilder(pool)
			nameBldr.AppendValues(names, nil)
			nameCol := nameBldr.NewStringArray()

			batch := array.NewRecord(schema, []arrow.Array{idCol, nameCol}, 10)
			batches <- batch
		}
	}()

	var buf bytes.Buffer
	pw := NewWriter(DefaultWriterConfig())
	if err := pw.WriteStream(&buf, schema, batches); err != nil {
		t.Fatalf("WriteStream failed: %v", err)
	}

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		t.Fatalf("create arrow reader: %v", err)
	}

	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 100 {
		t.Fatalf("expected 100 rows, got %d", tbl.NumRows())
	}
}
