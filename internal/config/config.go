// Package config loads the YAML file that drives which venues and symbols
// tickstore runs, plus the ClickHouse and sink settings.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole tickstore configuration.
type Config struct {
	ClickHouse ClickHouse `yaml:"clickhouse"`
	Sink       Sink       `yaml:"sink"`
	Metrics    Metrics    `yaml:"metrics"`
	Venues     []Venue    `yaml:"venues"`
}

// Metrics configures the Prometheus endpoint. An empty Addr disables it.
type Metrics struct {
	Addr string `yaml:"addr"`
}

// ClickHouse is the sink's connection. An empty Addr means "print to stdout"
// instead of writing to ClickHouse.
type ClickHouse struct {
	Addr     string `yaml:"addr"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Sink tunes the batcher. Zero fields fall back to the batcher's own defaults.
type Sink struct {
	MaxRows  int      `yaml:"max_rows"`
	MaxDelay Duration `yaml:"max_delay"`
	Buffer   int      `yaml:"buffer"`
}

// Venue is one exchange and the symbols to stream from it.
type Venue struct {
	Name    string   `yaml:"name"`
	Symbols []string `yaml:"symbols"`
}

// Duration is a time.Duration that unmarshals from a YAML string like "250ms".
type Duration time.Duration

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Load reads and validates the config at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

// validate checks the structural invariants. Venue names aren't checked here so
// config stays decoupled from the venue packages; main reports an unknown venue.
func (c *Config) validate() error {
	if len(c.Venues) == 0 {
		return fmt.Errorf("no venues configured")
	}
	seen := make(map[string]bool)
	for i, v := range c.Venues {
		if v.Name == "" {
			return fmt.Errorf("venue %d has no name", i)
		}
		if seen[v.Name] {
			return fmt.Errorf("venue %q listed twice", v.Name)
		}
		seen[v.Name] = true
		if len(v.Symbols) == 0 {
			return fmt.Errorf("venue %q has no symbols", v.Name)
		}
	}
	return nil
}
