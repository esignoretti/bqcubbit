package bigquery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Reader interface {
	ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error)
	ReadTableAtSnapshot(ctx context.Context, projectID, dataset, table string, snapshotTime time.Time) (<-chan arrow.Record, error)
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

func tablePath(projectID, dataset, table string) string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, dataset, table)
}

func (r *StorageReadReader) Schema(ctx context.Context, projectID, dataset, table string) (*arrow.Schema, error) {
	session, err := r.client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath(projectID, dataset, table),
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create read session for schema: %w", err)
	}
	arrowSchema := session.GetArrowSchema()
	if arrowSchema == nil {
		return nil, errors.New("no arrow schema returned")
	}
	sr, err := ipc.NewReader(bytes.NewReader(arrowSchema.GetSerializedSchema()))
	if err != nil {
		return nil, fmt.Errorf("parse arrow schema: %w", err)
	}
	defer sr.Release()
	return sr.Schema(), nil
}

func (r *StorageReadReader) ReadTable(ctx context.Context, projectID, dataset, table string) (<-chan arrow.Record, error) {
	return r.readTable(ctx, projectID, dataset, table, nil)
}

func (r *StorageReadReader) ReadTableAtSnapshot(ctx context.Context, projectID, dataset, table string, snapshotTime time.Time) (<-chan arrow.Record, error) {
	return r.readTable(ctx, projectID, dataset, table, &snapshotTime)
}

func (r *StorageReadReader) readTable(ctx context.Context, projectID, dataset, table string, snapshotTime *time.Time) (<-chan arrow.Record, error) {
	req := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath(projectID, dataset, table),
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 1,
	}
	if snapshotTime != nil {
		req.ReadSession.TableModifiers = &storagepb.ReadSession_TableModifiers{
			SnapshotTime: timestamppb.New(*snapshotTime),
		}
	}
	session, err := r.client.CreateReadSession(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create read session: %w", err)
	}

	if len(session.Streams) == 0 {
		return nil, errors.New("no streams returned")
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
			log.Printf("[bigquery] read rows stream: %v", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := readStream.Recv()
			if err != nil {
				if isStreamDone(err) {
					return
				}
				log.Printf("[bigquery] recv error: %v", err)
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
			ar, err := ipc.NewReader(combined)
			if err != nil {
				log.Printf("[bigquery] ipc reader error: %v", err)
				return
			}
			for ar.Next() {
				rec := ar.Record()
				rec.Retain()
				select {
				case out <- rec:
				case <-ctx.Done():
					rec.Release()
					return
				}
			}
			ar.Release()
		}
	}()
	return out, nil
}

func isStreamDone(err error) bool {
	return errors.Is(err, io.EOF) || status.Code(err) == codes.OutOfRange ||
		errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}
