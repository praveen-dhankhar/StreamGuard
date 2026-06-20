package parser

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

var sseContentFrame = []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}\n\n")
var sseStatusFrame = []byte("event: gateway_status\ndata: {\"state\":\"healthy\",\"provider\":\"openai\"}\n\n")
var sseDoneFrame = []byte("data: [DONE]\n\n")

func BenchmarkParseFrameContent(b *testing.B) {
	b.SetBytes(int64(len(sseContentFrame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(sseContentFrame)
	}
}

func BenchmarkParseFrameStatus(b *testing.B) {
	b.SetBytes(int64(len(sseStatusFrame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(sseStatusFrame)
	}
}

func BenchmarkParseFrameDone(b *testing.B) {
	b.SetBytes(int64(len(sseDoneFrame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(sseDoneFrame)
	}
}

func BenchmarkParseFrameLargeContent(b *testing.B) {
	// Simulate a larger content chunk (~500 bytes of text)
	largeText := bytes.Repeat([]byte("a"), 500)
	frame := append([]byte("data: {\"choices\":[{\"delta\":{\"content\":\""), largeText...)
	frame = append(frame, []byte("\"}}]}\n\n")...)
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(frame)
	}
}

type infiniteSSEReader struct {
	frame []byte
	buf   *bytes.Reader
}

func newInfiniteSSEReader(frame []byte) *infiniteSSEReader {
	return &infiniteSSEReader{frame: frame, buf: bytes.NewReader(frame)}
}

func (r *infiniteSSEReader) Read(p []byte) (int, error) {
	n, err := r.buf.Read(p)
	if err == io.EOF {
		r.buf.Reset(r.frame)
		if n == 0 {
			return r.buf.Read(p)
		}
		return n, nil
	}
	return n, err
}

func BenchmarkReaderNext(b *testing.B) {
	r := NewReader(newInfiniteSSEReader(sseContentFrame), nil)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.Next(ctx, time.Second)
	}
}
