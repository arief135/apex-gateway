package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root application configuration.
type Config struct {
	Gateway  GatewayConfig  `yaml:"gateway"`
	Admin    AdminConfig    `yaml:"admin"`
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Log      LogConfig      `yaml:"log"`
}

type GatewayConfig struct {
	Addr            string        `yaml:"addr"`             // e.g. :8080
	TLSAddr         string        `yaml:"tls_addr"`         // e.g. :8443
	TLSCertFile     string        `yaml:"tls_cert_file"`
	TLSKeyFile      string        `yaml:"tls_key_file"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	MaxHeaderBytes  int           `yaml:"max_header_bytes"`
	RouteConfigTTL  time.Duration `yaml:"route_config_ttl"` // how often to refresh routes from DB
}

type AdminConfig struct {
	Addr      string `yaml:"addr"`       // e.g. :9090
	JWTSecret string `yaml:"jwt_secret"`
}

type PostgresConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	Enabled bool     `yaml:"enabled"`
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, console
}

// Load reads config from a YAML file, then applies environment variable overrides.
func Load(path string) (*Config, error) {
	cfg := defaults()

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	if err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parsing config file: %w", err)
		}
	}

	applyEnv(cfg)
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Gateway: GatewayConfig{
			Addr:           ":8080",
			TLSAddr:        ":8443",
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    60 * time.Second,
			MaxHeaderBytes: 1 << 20, // 1MB
			RouteConfigTTL: 30 * time.Second,
		},
		Admin: AdminConfig{
			Addr:      ":9090",
			JWTSecret: "change-me-in-production",
		},
		Postgres: PostgresConfig{
			DSN:             "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable",
			MaxOpenConns:    25,
			MaxIdleConns:    10,
			ConnMaxLifetime: 5 * time.Minute,
		},
		Redis: RedisConfig{
			Addr: "localhost:6379",
			DB:   0,
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "gateway.traffic",
			Enabled: false,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// applyEnv overlays environment variables on top of the parsed config.
// All env vars use the GATEWAY_ prefix.
func applyEnv(cfg *Config) {
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		cfg.Gateway.Addr = v
	}
	if v := os.Getenv("GATEWAY_ADMIN_ADDR"); v != "" {
		cfg.Admin.Addr = v
	}
	if v := os.Getenv("GATEWAY_JWT_SECRET"); v != "" {
		cfg.Admin.JWTSecret = v
	}
	if v := os.Getenv("GATEWAY_POSTGRES_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("GATEWAY_REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("GATEWAY_REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	if v := os.Getenv("GATEWAY_KAFKA_ENABLED"); v != "" {
		cfg.Kafka.Enabled, _ = strconv.ParseBool(v)
	}
	if v := os.Getenv("GATEWAY_LOG_LEVEL"); v != "" {
		cfg.Log.Level = v
	}
}
