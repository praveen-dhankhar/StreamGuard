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

func TestAnthropicSplitFrameDoesNotMalformed(t *testing.T) {
	raw := []byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n")
	r := &chunkedReader{chunks: [][]byte{raw[:20], raw[20:67], raw[67:]}}
	frame, err := NewReaderForProvider(r, nil, "anthropic").Next(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("anthropic split frame parsed with error: %v", err)
	}
	if frame.Text != "hello" {
		t.Fatalf("text = %q, want hello", frame.Text)
	}
}

func TestAnthropicControlAndUsageFrames(t *testing.T) {
	ping, err := ParseFrameForProvider([]byte("event: ping\ndata: {\"type\":\"ping\"}\n\n"), "anthropic")
	if err != nil {
		t.Fatalf("ping parsed with error: %v", err)
	}
	if ping.Event != "ping" || ping.Text != "" {
		t.Fatalf("bad ping frame: %+v", ping)
	}

	usage, err := ParseFrameForProvider([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":14}}\n\n"), "anthropic")
	if err != nil {
		t.Fatalf("usage parsed with error: %v", err)
	}
	if !usage.HasUsage || usage.UsageTokens != 14 {
		t.Fatalf("bad usage frame: %+v", usage)
	}

	done, err := ParseFrameForProvider([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"), "anthropic")
	if err != nil {
		t.Fatalf("message_stop parsed with error: %v", err)
	}
	if done.Event != "done" {
		t.Fatalf("event = %q, want done", done.Event)
	}
}
