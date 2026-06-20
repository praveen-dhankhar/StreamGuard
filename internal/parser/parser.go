package parser

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"streamguard/internal/calibration"
	"streamguard/internal/protocol"
)

var ErrMalformed = errors.New("malformed")
var ErrSilentHang = errors.New("silent_hang")

type Frame struct {
	Event       string
	Data        []byte
	Text        string
	UsageTokens int
	HasUsage    bool
}

type Reader struct {
	br          *bufio.Reader
	cal         *calibration.Logger
	lastFrameAt time.Time
	format      string
}

func NewReader(r io.Reader, cal *calibration.Logger) *Reader {
	return NewReaderForProvider(r, cal, "openai")
}

func NewReaderForProvider(r io.Reader, cal *calibration.Logger, format string) *Reader {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" || format == "mock" {
		format = "openai"
	}
	return &Reader{br: bufio.NewReader(r), cal: cal, format: format}
}

func (r *Reader) Next(ctx context.Context, deadline time.Duration) (Frame, error) {
	type result struct {
		frame Frame
		err   error
	}
	ch := make(chan result, 1)
	activity := make(chan struct{}, 1)
	go func() {
		frame, err := r.readFrameWithActivity(activity)
		ch <- result{frame: frame, err: err}
	}()

	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		case <-timer.C:
			return Frame{}, ErrSilentHang
		case <-activity:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(deadline)
		case res := <-ch:
			if res.err == nil {
				now := time.Now()
				if !r.lastFrameAt.IsZero() && r.cal != nil {
					r.cal.Sample("inter_token_gap", float64(now.Sub(r.lastFrameAt).Milliseconds()))
				}
				r.lastFrameAt = now
			}
			return res.frame, res.err
		}
	}
}

func (r *Reader) readFrame() (Frame, error) {
	return r.readFrameWithActivity(nil)
}

func (r *Reader) readFrameWithActivity(activity chan<- struct{}) (Frame, error) {
	var buf bytes.Buffer
	for {
		b, err := r.br.ReadByte()
		if err == nil {
			buf.WriteByte(b)
			if activity != nil {
				select {
				case activity <- struct{}{}:
				default:
				}
			}
			if bytes.HasSuffix(buf.Bytes(), []byte("\n\n")) || bytes.HasSuffix(buf.Bytes(), []byte("\r\n\r\n")) {
				return ParseFrameForProvider(buf.Bytes(), r.format)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && buf.Len() > 0 {
				return ParseFrameForProvider(buf.Bytes(), r.format)
			}
			return Frame{}, err
		}
	}
}

func ParseFrame(raw []byte) (Frame, error) {
	return ParseFrameForProvider(raw, "openai")
}

func ParseFrameForProvider(raw []byte, format string) (Frame, error) {
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	var frame Frame
	var data []string
	for _, line := range lines {
		if strings.HasPrefix(line, "event:") {
			frame.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if len(data) == 0 {
		return Frame{}, ErrMalformed
	}
	frame.Data = []byte(strings.Join(data, "\n"))
	if strings.TrimSpace(string(frame.Data)) == "[DONE]" {
		frame.Event = "done"
		return frame, nil
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "anthropic" {
		return parseAnthropicFrame(frame)
	}
	if frame.Event == "" {
		text, err := extractContent(frame.Data)
		if err != nil {
			return Frame{}, ErrMalformed
		}
		frame.Text = text
		return frame, nil
	}
	if err := validateGatewayEvent(frame); err != nil {
		return Frame{}, ErrMalformed
	}
	return frame, nil
}

func parseAnthropicFrame(frame Frame) (Frame, error) {
	switch frame.Event {
	case "content_block_delta":
		var payload struct {
			Delta struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"delta"`
		}
		if err := json.Unmarshal(frame.Data, &payload); err != nil {
			return Frame{}, ErrMalformed
		}
		if payload.Delta.Type == "text_delta" {
			frame.Event = ""
			frame.Text = payload.Delta.Text
		}
		return frame, nil
	case "message_delta":
		var payload struct {
			Usage struct {
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(frame.Data, &payload); err != nil {
			return Frame{}, ErrMalformed
		}
		if payload.Usage.OutputTokens > 0 {
			frame.UsageTokens = payload.Usage.OutputTokens
			frame.HasUsage = true
		}
		return frame, nil
	case "message_stop":
		frame.Event = "done"
		return frame, nil
	case "error":
		return Frame{}, ErrMalformed
	case "message_start", "content_block_start", "content_block_stop", "ping":
		return frame, nil
	default:
		return frame, nil
	}
}

func extractContent(data []byte) (string, error) {
	var payload struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", err
	}
	if len(payload.Choices) == 0 {
		return "", nil
	}
	return payload.Choices[0].Delta.Content, nil
}

func validateGatewayEvent(frame Frame) error {
	switch frame.Event {
	case protocol.EventStatus:
		var v protocol.StatusData
		if err := json.Unmarshal(frame.Data, &v); err != nil || v.State != "healthy" || v.Provider == "" {
			return ErrMalformed
		}
	case protocol.EventFailover:
		var v protocol.FailoverData
		if err := json.Unmarshal(frame.Data, &v); err != nil || !protocol.ValidateFailoverReason(v.Reason) || v.Attempt < 1 {
			return ErrMalformed
		}
	case protocol.EventRegenerating:
		var v protocol.RegeneratingData
		if err := json.Unmarshal(frame.Data, &v); err != nil || !v.KeepPartialVisible {
			return ErrMalformed
		}
	case protocol.EventTruncated:
		var v protocol.TruncatedData
		if err := json.Unmarshal(frame.Data, &v); err != nil || !protocol.ValidateTruncatedReason(v.Reason) || !v.Final {
			return ErrMalformed
		}
	default:
		return ErrMalformed
	}
	return nil
}
