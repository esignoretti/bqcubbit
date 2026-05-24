package storage

import (
	"context"
	"strings"
	"testing"
)

func TestNewClient(t *testing.T) {
	_, err := NewClient(context.Background(), "https://s3.cubbit.eu", "ak", "sk", "test-bucket", "prefix/")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
}

func TestUploadStream(t *testing.T) {
	t.Skip("requires real Cubbit DS3 credentials")
	client, err := NewClient(context.Background(), "https://s3.cubbit.eu", "ak", "sk", "test-bucket", "test-prefix/")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.NewReader("test data")
	if err := client.UploadStream(context.Background(), "test.txt", body); err != nil {
		t.Fatalf("UploadStream failed: %v", err)
	}
}
