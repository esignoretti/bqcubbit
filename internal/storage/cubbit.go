package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
