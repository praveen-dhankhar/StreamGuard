package parser

import (
	"context"
	"io"
	"testing"
	"time"
)

type chunkedReader struct {
	chunks [][]byte
	delay  time.Duration
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
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

func TestSilentHangDeadlineResetsOnEveryReceivedByte(t *testing.T) {
	raw := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"héllo\"}}]}\n\n")
	chunks := make([][]byte, 0, len(raw))
	for _, b := range raw {
		chunks = append(chunks, []byte{b})
	}
	r := &chunkedReader{chunks: chunks, delay: 10 * time.Millisecond}
	frame, err := NewReader(r, nil).Next(context.Background(), 30*time.Millisecond)
	if err != nil {
		t.Fatalf("one-byte-at-a-time frame parsed with error: %v", err)
	}
	if frame.Text != "héllo" {
		t.Fatalf("text = %q, want héllo", frame.Text)
	}
}
