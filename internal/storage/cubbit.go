package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const uploadPartSize = 16 * 1024 * 1024

type Client struct {
	s3Client *s3.Client
	bucket   string
	prefix   string
}

func NewClient(ctx context.Context, endpoint, accessKey, secretKey, bucket, prefix string) (*Client, error) {
	s3Endpoint := endpoint
	if !strings.HasPrefix(s3Endpoint, "http://") && !strings.HasPrefix(s3Endpoint, "https://") {
		s3Endpoint = "https://" + s3Endpoint
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})

	return &Client{
		s3Client: client,
		bucket:   bucket,
		prefix:   strings.TrimSuffix(prefix, "/"),
	}, nil
}

func (c *Client) UploadStream(ctx context.Context, key string, body io.Reader) error {
	fullKey := c.prefix + "/" + key
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.bucket,
		Key:    &fullKey,
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("upload %s: %w", fullKey, err)
	}
	return nil
}

func (c *Client) ObjectExists(ctx context.Context, key string) (bool, error) {
	fullKey := c.prefix + "/" + key
	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    &fullKey,
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (c *Client) UploadMultipart(ctx context.Context, key string, body io.Reader) (string, error) {
	fullKey := c.prefix + "/" + key

	createResp, err := c.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    &fullKey,
	})
	if err != nil {
		return "", fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := *createResp.UploadId

	var parts []types.CompletedPart
	buf := make([]byte, uploadPartSize)
	var partNumber int32 = 0

	for {
		n, readErr := io.ReadFull(body, buf)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			c.AbortMultipartUpload(ctx, key, uploadID)
			return "", fmt.Errorf("read body: %w", readErr)
		}
		if n == 0 {
			break
		}
		partNumber++

		uploadResp, err := c.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     &c.bucket,
			Key:        &fullKey,
			PartNumber: &partNumber,
			UploadId:   &uploadID,
			Body:       bytes.NewReader(buf[:n]),
		})
		if err != nil {
			c.AbortMultipartUpload(ctx, key, uploadID)
			return "", fmt.Errorf("upload part %d: %w", partNumber, err)
		}

		parts = append(parts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: &partNumber,
		})
	}

	completeResp, err := c.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    &fullKey,
		UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		c.AbortMultipartUpload(ctx, key, uploadID)
		return "", fmt.Errorf("complete multipart upload: %w", err)
	}

	etag := *completeResp.ETag
	return etag, nil
}

func (c *Client) RenameObject(ctx context.Context, oldKey, newKey string) error {
	oldFull := c.prefix + "/" + oldKey
	newFull := c.prefix + "/" + newKey

	_, err := c.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &c.bucket,
		CopySource: aws.String(c.bucket + "/" + oldFull),
		Key:        &newFull,
	})
	if err != nil {
		return fmt.Errorf("copy object: %w", err)
	}

	_, err = c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    &oldFull,
	})
	if err != nil {
		return fmt.Errorf("delete old object: %w", err)
	}

	return nil
}

func (c *Client) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := c.prefix + "/" + prefix
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(c.s3Client, &s3.ListObjectsV2Input{
		Bucket: &c.bucket,
		Prefix: &fullPrefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}

	return keys, nil
}

func (c *Client) AbortStaleUploads(ctx context.Context, maxAge time.Duration) error {
	paginator := s3.NewListMultipartUploadsPaginator(c.s3Client, &s3.ListMultipartUploadsInput{
		Bucket: &c.bucket,
	})
	now := time.Now()
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list multipart uploads: %w", err)
		}
		for _, upload := range page.Uploads {
			if upload.Initiated != nil && now.Sub(*upload.Initiated) > maxAge {
				_, err := c.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   &c.bucket,
					Key:      upload.Key,
					UploadId: upload.UploadId,
				})
				if err != nil {
					log.Printf("[storage] warning: abort stale upload %s: %v", *upload.Key, err)
				} else {
					log.Printf("[storage] aborted stale multipart upload: %s", *upload.Key)
				}
			}
		}
	}
	return nil
}

func (c *Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	fullKey := c.prefix + "/" + key
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &fullKey,
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", fullKey, err)
	}
	return resp.Body, nil
}

func (c *Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	fullKey := c.prefix + "/" + key
	_, err := c.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      &fullKey,
		UploadId: &uploadID,
	})
	return err
}
