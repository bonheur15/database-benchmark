package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases         Databases         `yaml:"databases"`
	BenchmarkSettings BenchmarkSettings `yaml:"benchmark_settings"`
}

type Databases struct {
	Postgres string `yaml:"postgres"`
	MySQL    string `yaml:"mysql"`
	Mongo    string `yaml:"mongo"`
}

type BenchmarkSettings struct {
	DefaultDuration    string `yaml:"default_duration"`
	DefaultConcurrency int    `yaml:"default_concurrency"`
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{}

	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
