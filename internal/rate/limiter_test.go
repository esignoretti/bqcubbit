package rate

import (
	"context"
	"testing"
	"time"
)

func TestHighRateDoesNotBlock(t *testing.T) {
	l := NewLimiters(3600, 3600, 60)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := l.WaitBQRead(ctx); err != nil {
		t.Fatalf("high-rate WaitBQRead blocked: %v", err)
	}
	if err := l.WaitBQExport(ctx); err != nil {
		t.Fatalf("high-rate WaitBQExport blocked: %v", err)
	}
	if err := l.WaitUpload(ctx); err != nil {
		t.Fatalf("high-rate WaitUpload blocked: %v", err)
	}
}

func TestSlowRateDoesBlock(t *testing.T) {
	l := NewLimiters(0, 0, 0)
	// First token should be available initially (burst=1)
	ctx := context.Background()
	if err := l.WaitBQRead(ctx); err != nil {
		t.Fatalf("first WaitBQRead should not block: %v", err)
	}
	// Second call must block since rate is 0 and burst=1
	ctxTimed, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.WaitBQRead(ctxTimed); err == nil {
		t.Fatal("slow-rate WaitBQRead should have blocked")
	}
}

func TestSlowRateExportDoesBlock(t *testing.T) {
	l := NewLimiters(0, 0, 0)
	ctx := context.Background()
	if err := l.WaitBQExport(ctx); err != nil {
		t.Fatalf("first WaitBQExport should not block: %v", err)
	}
	ctxTimed, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.WaitBQExport(ctxTimed); err == nil {
		t.Fatal("slow-rate WaitBQExport should have blocked")
	}
}

func TestSlowRateUploadDoesBlock(t *testing.T) {
	l := NewLimiters(0, 0, 0)
	ctx := context.Background()
	if err := l.WaitUpload(ctx); err != nil {
		t.Fatalf("first WaitUpload should not block: %v", err)
	}
	ctxTimed, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.WaitUpload(ctxTimed); err == nil {
		t.Fatal("slow-rate WaitUpload should have blocked")
	}
}
