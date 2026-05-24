package hash

import (
	"io"
	"strings"
	"testing"
)

func TestReader(t *testing.T) {
	input := "hello world"
	r := NewReader(strings.NewReader(input))

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if got := string(out); got != input {
		t.Fatalf("got %q, want %q", got, input)
	}

	if r.TotalBytes() != 11 {
		t.Fatalf("TotalBytes = %d, want 11", r.TotalBytes())
	}

	wantSHA := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if got := r.SHA256(); got != wantSHA {
		t.Fatalf("SHA256 = %q, want %q", got, wantSHA)
	}
}
