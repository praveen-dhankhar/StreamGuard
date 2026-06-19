package tokenizer

import (
	"log"
	"strings"
	"sync"

	tiktoken "github.com/pkoukk/tiktoken-go"
)

type Counter interface {
	Count(provider, model, hint, text string) int
}

type ChunkCounter struct{}

func (ChunkCounter) Count(_ string, _ string, _ string, text string) int {
	if text == "" {
		return 0
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 1
	}
	return len(fields)
}

type ProviderAwareCounter struct {
	openAI    *OpenAICounter
	anthropic *AnthropicCounter
	fallback  ChunkCounter
}

type OpenAICounter struct {
	mu    sync.Mutex
	cache map[string]*tiktoken.Tiktoken
}

type AnthropicCounter struct {
	encoding *tiktoken.Tiktoken
}

func NewProviderAwareCounter(reg *Registry) Counter {
	if reg != nil {
		reg.Register("openai", "tiktoken-go")
		reg.Register("anthropic", "cl100k-base-fallback")
	}
	encoding, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		encoding = nil
	}
	return &ProviderAwareCounter{
		openAI: &OpenAICounter{cache: make(map[string]*tiktoken.Tiktoken)},
		anthropic: &AnthropicCounter{
			encoding: encoding,
		},
		fallback: ChunkCounter{},
	}
}

func (c *ProviderAwareCounter) Count(provider, model, hint, text string) int {
	if strings.EqualFold(strings.TrimSpace(hint), "mock-chunk-v1") {
		return c.fallback.Count(provider, model, hint, text)
	}
	name := strings.ToLower(provider)
	switch {
	case strings.Contains(name, "openai"):
		return c.openAI.Count(provider, model, hint, text)
	case strings.Contains(name, "anthropic"):
		return c.anthropic.Count(provider, model, hint, text)
	default:
		return c.fallback.Count(provider, model, hint, text)
	}
}

func (c *OpenAICounter) Count(_ string, model, _ string, text string) int {
	if text == "" {
		return 0
	}
	enc := c.encodingForModel(model)
	if enc == nil {
		return ChunkCounter{}.Count("", model, "", text)
	}
	return len(enc.Encode(text, nil, nil))
}

func (c *OpenAICounter) encodingForModel(model string) *tiktoken.Tiktoken {
	key := strings.TrimSpace(model)
	if key == "" {
		key = "cl100k_base"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if enc, ok := c.cache[key]; ok {
		return enc
	}
	enc, err := tiktoken.EncodingForModel(key)
	if err != nil {
		enc, err = tiktoken.GetEncoding("cl100k_base")
		if err != nil {
			return nil
		}
	}
	c.cache[key] = enc
	return enc
}

func (c *AnthropicCounter) Count(_ string, _ string, _ string, text string) int {
	if text == "" {
		return 0
	}
	if c.encoding == nil {
		return ChunkCounter{}.Count("", "", "", text)
	}
	return len(c.encoding.Encode(text, nil, nil))
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

func (r *Registry) Snapshot(provider string) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[provider]
	if e == nil {
		return Entry{}, false
	}
	return *e, true
}

func LogIfSuspected(provider string, suspected bool) {
	if suspected {
		log.Printf("tokenizer_drift_suspected provider=%s", provider)
	}
}
