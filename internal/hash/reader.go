package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type Reader struct {
	reader io.Reader
	hash   hash.Hash
	total  int64
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: r,
		hash:   sha256.New(),
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.hash.Write(p[:n])
		r.total += int64(n)
	}
	return n, err
}

func (r *Reader) SHA256() string {
	return hex.EncodeToString(r.hash.Sum(nil))
}

func (r *Reader) TotalBytes() int64 {
	return r.total
}
