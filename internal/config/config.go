package config

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"streamguard/internal/breaker"
)

type Config struct {
	CircuitBreaker  BreakerConfig
	Providers       []Provider
	Timeouts        Timeouts
	Reconciliation  Reconciliation
	RateLimit       RateLimit
	Budget          Budget
	Auth            Auth
	Shutdown        Shutdown
	OpenAIAPIKey    string
	AnthropicAPIKey string
	OperatorToken   string
}

type BreakerConfig struct {
	FailureThreshold         int
	OpenTimeoutSeconds       int
	HalfOpenSuccessThreshold int
}

type Provider struct {
	Name           string
	Type           string
	Priority       int
	BaseURL        string
	CircuitBreaker *BreakerConfig
}

func (p Provider) ProviderType() string {
	typ := strings.ToLower(strings.TrimSpace(p.Type))
	if typ == "" {
		return "mock"
	}
	return typ
}

type Timeouts struct {
	SilentHangDeadlineMS int
}

type Reconciliation struct {
	Interval          time.Duration
	DriftThresholdPct float64
}

type RateLimit struct {
	WindowSeconds int
	MaxTokens     int64
}

type Budget struct {
	DefaultPeriod time.Duration
}

type Auth struct {
	KeysFile string
}

type Shutdown struct {
	DrainTimeoutSeconds int
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	if err := parseFile(path, &cfg); err != nil {
		return Config{}, err
	}
	cfg.OpenAIAPIKey = os.Getenv("OPENAI_API_KEY")
	cfg.AnthropicAPIKey = os.Getenv("ANTHROPIC_API_KEY")
	cfg.OperatorToken = os.Getenv("OPERATOR_TOKEN")
	if cfg.OperatorToken == "" {
		cfg.OperatorToken = "dev-operator-token"
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	sort.Slice(cfg.Providers, func(i, j int) bool { return cfg.Providers[i].Priority < cfg.Providers[j].Priority })
	return cfg, nil
}

func Defaults() Config {
	return Config{
		CircuitBreaker: BreakerConfig{FailureThreshold: 3, OpenTimeoutSeconds: 30, HalfOpenSuccessThreshold: 1},
		Timeouts:       Timeouts{SilentHangDeadlineMS: 1000},
		Reconciliation: Reconciliation{Interval: time.Hour, DriftThresholdPct: 1.12},
		RateLimit:      RateLimit{WindowSeconds: 60, MaxTokens: 1000000},
		Budget:         Budget{DefaultPeriod: 24 * time.Hour},
		Auth:           Auth{KeysFile: "./keys.yaml"},
		Shutdown:       Shutdown{DrainTimeoutSeconds: 30},
	}
}

func (c Config) BreakerConfigFor(p Provider) breaker.Config {
	bc := c.CircuitBreaker
	if p.CircuitBreaker != nil {
		if p.CircuitBreaker.FailureThreshold != 0 {
			bc.FailureThreshold = p.CircuitBreaker.FailureThreshold
		}
		if p.CircuitBreaker.OpenTimeoutSeconds != 0 {
			bc.OpenTimeoutSeconds = p.CircuitBreaker.OpenTimeoutSeconds
		}
		if p.CircuitBreaker.HalfOpenSuccessThreshold != 0 {
			bc.HalfOpenSuccessThreshold = p.CircuitBreaker.HalfOpenSuccessThreshold
		}
	}
	return breaker.Config{
		FailureThreshold:         bc.FailureThreshold,
		OpenTimeout:              time.Duration(bc.OpenTimeoutSeconds) * time.Second,
		HalfOpenSuccessThreshold: bc.HalfOpenSuccessThreshold,
	}
}

func (c Config) Validate() error {
	if len(c.Providers) == 0 {
		return errors.New("at least one provider is required")
	}
	names := map[string]bool{}
	priorities := map[int]bool{}
	for _, p := range c.Providers {
		if p.Name == "" {
			return errors.New("provider name cannot be empty")
		}
		if names[p.Name] {
			return fmt.Errorf("duplicate provider name %q", p.Name)
		}
		names[p.Name] = true
		switch p.ProviderType() {
		case "mock", "openai", "anthropic":
		default:
			return fmt.Errorf("provider %q type must be mock, openai, or anthropic", p.Name)
		}
		if p.Priority < 0 {
			return fmt.Errorf("provider %q priority cannot be negative", p.Name)
		}
		if priorities[p.Priority] {
			return fmt.Errorf("duplicate provider priority %d", p.Priority)
		}
		priorities[p.Priority] = true
		u, err := url.Parse(p.BaseURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("provider %q base_url must be a valid URL", p.Name)
		}
		if err := validateBreaker(c.BreakerConfigFor(p)); err != nil {
			return fmt.Errorf("provider %q circuit breaker invalid: %w", p.Name, err)
		}
	}
	if err := validateBreaker(c.BreakerConfigFor(Provider{})); err != nil {
		return err
	}
	if c.RateLimit.WindowSeconds < 1 {
		return errors.New("rate_limit.window_s must be >= 1")
	}
	if c.RateLimit.MaxTokens < 1 {
		return errors.New("rate_limit.max_tokens must be >= 1")
	}
	if c.Reconciliation.Interval <= 0 {
		return errors.New("reconciliation.interval must be > 0")
	}
	if c.Shutdown.DrainTimeoutSeconds < 0 {
		return errors.New("shutdown.drain_timeout_s must be >= 0")
	}
	if c.Auth.KeysFile == "" {
		return errors.New("auth.keys_file is required")
	}
	if _, err := os.Stat(c.Auth.KeysFile); err != nil {
		return fmt.Errorf("auth.keys_file is invalid: %w", err)
	}
	return nil
}

func validateBreaker(cfg breaker.Config) error {
	if cfg.FailureThreshold < 1 {
		return errors.New("circuit_breaker.failure_threshold must be >= 1")
	}
	if cfg.OpenTimeout < time.Second {
		return errors.New("circuit_breaker.open_timeout_s must be >= 1")
	}
	if cfg.HalfOpenSuccessThreshold < 1 {
		return errors.New("circuit_breaker.half_open_success_threshold must be >= 1")
	}
	return nil
}

func parseFile(path string, cfg *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var section string
	var currentProvider *Provider
	var providerOverride bool
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		raw := scanner.Text()
		line := strings.TrimSpace(strings.Split(raw, "#")[0])
		if line == "" {
			continue
		}
		if !strings.HasPrefix(raw, " ") && strings.HasSuffix(line, ":") {
			section = strings.TrimSuffix(line, ":")
			currentProvider = nil
			providerOverride = false
			continue
		}
		if section == "providers" && strings.HasPrefix(strings.TrimSpace(raw), "- ") {
			cfg.Providers = append(cfg.Providers, Provider{})
			currentProvider = &cfg.Providers[len(cfg.Providers)-1]
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "- "))
			if line == "" {
				continue
			}
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), "\"")
		if section == "providers" && currentProvider != nil {
			if key == "circuit_breaker" {
				currentProvider.CircuitBreaker = &BreakerConfig{}
				providerOverride = true
				continue
			}
			if providerOverride {
				setBreakerValue(currentProvider.CircuitBreaker, key, val)
				continue
			}
			switch key {
			case "name":
				currentProvider.Name = val
			case "type":
				currentProvider.Type = val
			case "priority":
				currentProvider.Priority = atoi(val)
			case "base_url":
				currentProvider.BaseURL = val
			}
			continue
		}
		switch section {
		case "circuit_breaker":
			setBreakerValue(&cfg.CircuitBreaker, key, val)
		case "timeouts":
			if key == "silent_hang_deadline_ms" {
				cfg.Timeouts.SilentHangDeadlineMS = atoi(val)
			}
		case "reconciliation":
			if key == "interval" {
				d, err := time.ParseDuration(val)
				if err != nil {
					return err
				}
				cfg.Reconciliation.Interval = d
			}
			if key == "drift_threshold_pct" {
				cfg.Reconciliation.DriftThresholdPct = atof(val)
			}
		case "rate_limit":
			if key == "window_s" {
				cfg.RateLimit.WindowSeconds = atoi(val)
			}
			if key == "max_tokens" {
				cfg.RateLimit.MaxTokens = int64(atoi(val))
			}
		case "budget":
			if key == "default_period" {
				d, err := time.ParseDuration(val)
				if err != nil {
					return err
				}
				cfg.Budget.DefaultPeriod = d
			}
		case "auth":
			if key == "keys_file" {
				cfg.Auth.KeysFile = val
			}
		case "shutdown":
			if key == "drain_timeout_s" {
				cfg.Shutdown.DrainTimeoutSeconds = atoi(val)
			}
		}
	}
	return scanner.Err()
}

func setBreakerValue(cfg *BreakerConfig, key, val string) {
	switch key {
	case "failure_threshold":
		cfg.FailureThreshold = atoi(val)
	case "open_timeout_s":
		cfg.OpenTimeoutSeconds = atoi(val)
	case "half_open_success_threshold":
		cfg.HalfOpenSuccessThreshold = atoi(val)
	}
}

func atoi(v string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(v))
	return n
}

func atof(v string) float64 {
	n, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
	return n
}
