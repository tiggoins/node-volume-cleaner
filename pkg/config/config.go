package config

import (
	"encoding/json"
	"os"
	"time"
)

// Config holds the configuration for the volume cleaner controller
type Config struct {
	// WaitTimeout is the time to wait before cleaning volumes after node becomes NotReady
	WaitTimeout time.Duration `json:"waitTimeout"`
	
	// RecheckInterval is the interval to recheck node status before cleanup
	RecheckInterval time.Duration `json:"recheckInterval"`
	
	// MaxRetries is the maximum number of retries for failed operations
	MaxRetries int `json:"maxRetries"`
	
	// ExcludeNamespaces contains namespaces to exclude from cleanup
	ExcludeNamespaces []string `json:"excludeNamespaces"`
	
	// ExcludeNodeLabels contains node labels to exclude from processing
	ExcludeNodeLabels map[string]string `json:"excludeNodeLabels"`
	
	// DryRun enables dry-run mode (no actual cleanup)
	DryRun bool `json:"dryRun"`
	
	// EnableMetrics enables Prometheus metrics
	EnableMetrics bool `json:"enableMetrics"`
	
	// MetricsPort is the port for metrics server
	MetricsPort int `json:"metricsPort"`
	
	// LogLevel sets the log level
	LogLevel string `json:"logLevel"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		WaitTimeout:     5 * time.Minute,
		RecheckInterval: 30 * time.Second,
		MaxRetries:      3,
		ExcludeNamespaces: []string{
			"kube-system",
			"kube-public",
			"kube-node-lease",
		},
		ExcludeNodeLabels: map[string]string{
			"exclude-from-volume-cleaner": "true",
		},
		DryRun:        false,
		EnableMetrics: true,
		MetricsPort:   8080,
		LogLevel:      "info",
	}
}

// LoadConfig loads configuration from file or returns default
func LoadConfig(configFile string) (*Config, error) {
	cfg := DefaultConfig()
	
	if configFile == "" {
		return cfg, nil
	}
	
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	
	return cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.WaitTimeout <= 0 {
		c.WaitTimeout = 5 * time.Minute
	}
	
	if c.RecheckInterval <= 0 {
		c.RecheckInterval = 30 * time.Second
	}
	
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	
	if c.MetricsPort <= 0 {
		c.MetricsPort = 8080
	}
	
	return nil
}
