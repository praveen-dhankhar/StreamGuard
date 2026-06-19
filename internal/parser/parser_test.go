package parser

import (
	"context"
	"io"
	"testing"
	"time"
)

type chunkedReader struct {
	chunks [][]byte
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := r.chunks[0]
	r.chunks = r.chunks[1:]
	copy(p, chunk)
	return len(chunk), nil
}

func TestSplitFrameDoesNotMalformed(t *testing.T) {
	raw := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	r := &chunkedReader{chunks: [][]byte{raw[:12], raw[12:30], raw[30:]}}
	frame, err := NewReader(r, nil).Next(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("split frame parsed with error: %v", err)
	}
	if frame.Text != "hello" {
		t.Fatalf("text = %q, want hello", frame.Text)
	}
}
