package protocol

import (
	"encoding/json"
	"fmt"
	"io"
)

const (
	EventStatus       = "gateway_status"
	EventFailover     = "gateway_failover"
	EventRegenerating = "gateway_regenerating"
	EventTruncated    = "gateway_truncated"
)

type FailoverReason string

const (
	ReasonDeadSocket FailoverReason = "dead_socket"
	ReasonSilentHang FailoverReason = "silent_hang"
	ReasonMalformed  FailoverReason = "malformed"
)

type TruncatedReason string

const (
	ReasonAllProvidersExhausted TruncatedReason = "all_providers_exhausted"
	ReasonBudgetExceeded        TruncatedReason = "budget_exceeded"
)

type StatusData struct {
	State    string `json:"state"`
	Provider string `json:"provider"`
}

type FailoverData struct {
	Reason                       FailoverReason `json:"reason"`
	TokensDeliveredBeforeFailure int            `json:"tokens_delivered_before_failure"`
	ProviderFrom                 string         `json:"provider_from"`
	ProviderTo                   string         `json:"provider_to"`
	Attempt                      int            `json:"attempt"`
}

type RegeneratingData struct {
	KeepPartialVisible bool `json:"keep_partial_visible"`
}

type TruncatedData struct {
	Reason          TruncatedReason `json:"reason"`
	TokensDelivered int             `json:"tokens_delivered"`
	Final           bool            `json:"final"`
}

func ValidateFailoverReason(reason FailoverReason) bool {
	switch reason {
	case ReasonDeadSocket, ReasonSilentHang, ReasonMalformed:
		return true
	default:
		return false
	}
}

func ValidateTruncatedReason(reason TruncatedReason) bool {
	switch reason {
	case ReasonAllProvidersExhausted, ReasonBudgetExceeded:
		return true
	default:
		return false
	}
}

func WriteSSE(w io.Writer, event string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	return err
}

func WriteContent(w io.Writer, text string) error {
	b, err := json.Marshal(map[string]any{
		"choices": []map[string]any{{
			"delta": map[string]string{"content": text},
		}},
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	return err
}
