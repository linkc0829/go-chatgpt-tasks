// Package config loads application configuration from env / .env / yaml.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App    AppConfig
	HTTP   HTTPConfig
	DB     DBConfig
	Redis  RedisConfig
	JWT    JWTConfig
	OTel   OTelConfig
	Logger LoggerConfig
	Quota  QuotaConfig
	LLM    LLMConfig
}

type AppConfig struct {
	Env             string        `mapstructure:"env"`
	Name            string        `mapstructure:"name"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type HTTPConfig struct {
	Port int `mapstructure:"port"`
}

type DBConfig struct {
	DSN      string `mapstructure:"dsn"`
	MaxConns int32  `mapstructure:"max_conns"`
	MinConns int32  `mapstructure:"min_conns"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type JWTConfig struct {
	Secret string        `mapstructure:"secret"`
	Issuer string        `mapstructure:"issuer"`
	TTL    time.Duration `mapstructure:"ttl"`
}

type OTelConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	Endpoint    string `mapstructure:"endpoint"`
	ServiceName string `mapstructure:"service_name"`
}

type LoggerConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

type QuotaConfig struct {
	MaxJobsPerHour       int `mapstructure:"max_jobs_per_hour"`
	MaxActiveRecurring   int `mapstructure:"max_active_recurring_jobs"`
	MaxConcurrentRuns    int `mapstructure:"max_concurrent_runs"`
	MaxDailyLLMCostCents int `mapstructure:"max_daily_llm_cost_cents"`
}

type LLMConfig struct {
	TimeoutSeconds  int    `mapstructure:"timeout_seconds"`
	MaxRetries      int    `mapstructure:"max_retries"`
	MaxInputTokens  int    `mapstructure:"max_input_tokens"`
	MaxOutputTokens int    `mapstructure:"max_output_tokens"`
	MaxCostCents    int    `mapstructure:"max_cost_cents"`
	OutputSchema    string `mapstructure:"output_schema"`
}

// Load reads config from env (with .env fallback). Env vars are upper-cased
// and underscored, e.g. APP_ENV, POSTGRES_DSN.
func Load() (*Config, error) {
	return load(true)
}

// LoadMCP loads config for the stdio MCP server. MCP only needs database
// access, so it should not require HTTP auth settings like JWT_SECRET.
func LoadMCP() (*Config, error) {
	return load(false)
}

func load(requireJWT bool) (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("app.env", "development")
	v.SetDefault("app.name", "go-chatgpt-tasks")
	v.SetDefault("app.shutdown_timeout", "10s")
	v.SetDefault("http.port", 8080)
	v.SetDefault("db.max_conns", 20)
	v.SetDefault("db.min_conns", 2)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("jwt.issuer", "go-chatgpt-tasks")
	v.SetDefault("jwt.secret", "local-dev-secret")
	v.SetDefault("jwt.ttl", "24h")
	v.SetDefault("otel.enabled", false)
	v.SetDefault("otel.endpoint", "localhost:4317")
	v.SetDefault("otel.service_name", "go-chatgpt-tasks")
	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.encoding", "json")
	v.SetDefault("quota.max_jobs_per_hour", 100)
	v.SetDefault("quota.max_active_recurring_jobs", 20)
	v.SetDefault("quota.max_concurrent_runs", 10)
	v.SetDefault("quota.max_daily_llm_cost_cents", 1000)
	v.SetDefault("llm.timeout_seconds", 30)
	v.SetDefault("llm.max_retries", 3)
	v.SetDefault("llm.max_input_tokens", 4096)
	v.SetDefault("llm.max_output_tokens", 1024)
	v.SetDefault("llm.max_cost_cents", 100)
	v.SetDefault("llm.output_schema", "{}")

	// Env mapping: APP_ENV → app.env
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Manual binds (viper's automatic binding doesn't traverse nested keys
	// reliably for unset envs).
	binds := map[string]string{
		"app.env":                         "APP_ENV",
		"app.name":                        "APP_NAME",
		"app.shutdown_timeout":            "APP_SHUTDOWN_TIMEOUT",
		"http.port":                       "APP_PORT",
		"db.dsn":                          "POSTGRES_DSN",
		"db.max_conns":                    "POSTGRES_MAX_CONNS",
		"db.min_conns":                    "POSTGRES_MIN_CONNS",
		"redis.addr":                      "REDIS_ADDR",
		"redis.password":                  "REDIS_PASSWORD",
		"redis.db":                        "REDIS_DB",
		"jwt.secret":                      "JWT_SECRET",
		"jwt.issuer":                      "JWT_ISSUER",
		"jwt.ttl":                         "JWT_TTL",
		"otel.enabled":                    "OTEL_ENABLED",
		"otel.endpoint":                   "OTEL_ENDPOINT",
		"otel.service_name":               "OTEL_SERVICE_NAME",
		"logger.level":                    "LOG_LEVEL",
		"logger.encoding":                 "LOG_ENCODING",
		"quota.max_jobs_per_hour":         "QUOTA_MAX_JOBS_PER_HOUR",
		"quota.max_active_recurring_jobs": "QUOTA_MAX_ACTIVE_RECURRING_JOBS",
		"quota.max_concurrent_runs":       "QUOTA_MAX_CONCURRENT_RUNS",
		"quota.max_daily_llm_cost_cents":  "QUOTA_MAX_DAILY_LLM_COST_CENTS",
		"llm.timeout_seconds":             "LLM_TIMEOUT_SECONDS",
		"llm.max_retries":                 "LLM_MAX_RETRIES",
		"llm.max_input_tokens":            "LLM_MAX_INPUT_TOKENS",
		"llm.max_output_tokens":           "LLM_MAX_OUTPUT_TOKENS",
		"llm.max_cost_cents":              "LLM_MAX_COST_CENTS",
		"llm.output_schema":               "LLM_OUTPUT_SCHEMA",
	}
	for k, env := range binds {
		_ = v.BindEnv(k, env)
	}

	// Optional .env (developer convenience)
	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	_ = v.ReadInConfig() // ignore if missing
	for key, env := range binds {
		if _, ok := os.LookupEnv(env); ok {
			continue
		}
		if v.IsSet(env) {
			v.Set(key, v.Get(env))
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.validate(requireJWT); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate(requireJWT bool) error {
	if c.DB.DSN == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	if requireJWT && c.JWT.Secret == "" {
		return fmt.Errorf("JWT_SECRET is required")
	}
	return nil
}
