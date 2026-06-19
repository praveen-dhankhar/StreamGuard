package tokenizer

import (
	"strings"
	"sync"
)

type Counter interface {
	Count(provider, text string) int
}

type ChunkCounter struct{}

func (ChunkCounter) Count(_ string, text string) int {
	if text == "" {
		return 0
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 1
	}
	return len(fields)
}

type Registry struct {
	mu      sync.Mutex
	entries map[string]*Entry
}

type Entry struct {
	ProviderName              string
	PinnedVersion             string
	ConsecutiveAboveThreshold int
}

func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*Entry)}
}

func (r *Registry) Register(provider, version string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[provider] = &Entry{ProviderName: provider, PinnedVersion: version}
}

func (r *Registry) Observe(provider string, above bool) (suspected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[provider]
	if e == nil {
		e = &Entry{ProviderName: provider, PinnedVersion: "chunk-counter-v1"}
		r.entries[provider] = e
	}
	if above {
		e.ConsecutiveAboveThreshold++
	} else {
		e.ConsecutiveAboveThreshold = 0
	}
	return e.ConsecutiveAboveThreshold >= 3
}
