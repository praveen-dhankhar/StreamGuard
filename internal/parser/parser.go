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
	Event string
	Data  []byte
	Text  string
}

type Reader struct {
	br          *bufio.Reader
	cal         *calibration.Logger
	lastFrameAt time.Time
}

func NewReader(r io.Reader, cal *calibration.Logger) *Reader {
	return &Reader{br: bufio.NewReader(r), cal: cal}
}

func (r *Reader) Next(ctx context.Context, deadline time.Duration) (Frame, error) {
	type result struct {
		frame Frame
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		frame, err := r.readFrame()
		ch <- result{frame: frame, err: err}
	}()

	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-timer.C:
		return Frame{}, ErrSilentHang
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

func (r *Reader) readFrame() (Frame, error) {
	var buf bytes.Buffer
	for {
		line, err := r.br.ReadBytes('\n')
		if len(line) > 0 {
			buf.Write(line)
			if bytes.HasSuffix(buf.Bytes(), []byte("\n\n")) || bytes.HasSuffix(buf.Bytes(), []byte("\r\n\r\n")) {
				return ParseFrame(buf.Bytes())
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && buf.Len() > 0 {
				return ParseFrame(buf.Bytes())
			}
			return Frame{}, err
		}
	}
}

func ParseFrame(raw []byte) (Frame, error) {
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
