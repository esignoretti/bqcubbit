package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/api/option"
)

type Reader interface {
	ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error)
	Schema(ctx context.Context, projectID, dataset, table string) (*arrow.Schema, error)
	Close() error
}

type StorageReadReader struct {
	client   *storage.BigQueryReadClient
	project  string
	location string
}

func NewStorageReadReader(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*StorageReadReader, error) {
	client, err := storage.NewBigQueryReadClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create bq storage client: %w", err)
	}
	return &StorageReadReader{client: client, project: projectID, location: location}, nil
}

func (r *StorageReadReader) Close() error {
	return r.client.Close()
}

func (r *StorageReadReader) Schema(ctx context.Context, projectID, dataset, table string) (*arrow.Schema, error) {
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, dataset, table)
	session, err := r.client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath,
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create read session for schema: %w", err)
	}
	arrowSchema := session.GetArrowSchema()
	if arrowSchema == nil {
		return nil, fmt.Errorf("no arrow schema returned")
	}
	reader, err := ipc.NewReader(bytes.NewReader(arrowSchema.GetSerializedSchema()))
	if err != nil {
		return nil, fmt.Errorf("parse arrow schema: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

func (r *StorageReadReader) ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error) {
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, dataset, table)

	session, err := r.client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath,
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create read session: %w", err)
	}

	if len(session.Streams) == 0 {
		return nil, fmt.Errorf("no streams returned")
	}

	schemaBytes := session.GetArrowSchema().GetSerializedSchema()

	out := make(chan arrow.Record, 32)
	go func() {
		defer close(out)
		stream := session.Streams[0]
		readStream, err := r.client.ReadRows(ctx, &storagepb.ReadRowsRequest{
			ReadStream: stream.Name,
		})
		if err != nil {
			return
		}

		for {
			resp, err := readStream.Recv()
			if err != nil {
				return
			}

			batch := resp.GetArrowRecordBatch()
			if batch == nil {
				continue
			}

			combined := io.MultiReader(
				bytes.NewReader(schemaBytes),
				bytes.NewReader(batch.GetSerializedRecordBatch()),
			)
			reader, err := ipc.NewReader(combined)
			if err != nil {
				return
			}
			for reader.Next() {
				rec := reader.Record()
				rec.Retain()
				out <- rec
			}
			reader.Release()
		}
	}()
	return out, nil
}
