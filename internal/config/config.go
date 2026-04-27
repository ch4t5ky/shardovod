package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// OpenSearch
	OpenSearchAddresses []string // e.g. ["http://localhost:9200"]
	OpenSearchUsername  string
	OpenSearchPassword  string

	// Minecraft RCON
	RCONAddr     string
	RCONPassword string

	// Polling
	PollInterval time.Duration

	// Pen origin in the Minecraft world
	PenX int
	PenY int
	PenZ int
}

func New() (*Config, error) {
	cfg := &Config{
		OpenSearchAddresses: getEnvList("OPENSEARCH_ADDRESSES", []string{"http://localhost:9200"}),
		OpenSearchUsername:  getEnv("OPENSEARCH_USERNAME", ""),
		OpenSearchPassword:  getEnv("OPENSEARCH_PASSWORD", ""),
		RCONAddr:            getEnv("RCON_ADDR", "localhost:25575"),
		RCONPassword:        getEnv("RCON_PASSWORD", ""),
		PollInterval:        getEnvDuration("POLL_INTERVAL", 5*time.Second),
		PenX:                getEnvInt("PEN_X", 0),
		PenY:                getEnvInt("PEN_Y", 100),
		PenZ:                getEnvInt("PEN_Z", 20),
	}

	if cfg.RCONPassword == "" {
		return nil, fmt.Errorf("RCON_PASSWORD is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEnvList splits a comma-separated env var into a slice.
func getEnvList(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
