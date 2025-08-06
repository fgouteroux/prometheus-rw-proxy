package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var LabelValueRegex []CompiledMatcher

type LabelMatcher struct {
	Name     string `yaml:"name"`
	RegexStr string `yaml:"regex"`
}

type CompiledMatcher struct {
	Name  string
	Regex *regexp.Regexp
}

type config struct {
	Listen               string `yaml:"listen"`
	ListenPath           string `yaml:"listen_path"`
	ListenPprof          string `yaml:"listen_pprof"`
	ListenMetricsAddress string `yaml:"listen_metrics_address"`
	MetricsIncludeTenant bool   `yaml:"metrics_include_tenant"`

	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	Target     string `yaml:"target"`
	EnableIPv6 bool   `yaml:"enable_ipv6"`

	EnableHTTP2 bool `yaml:"enable_http2"`

	LogLevel           string        `yaml:"log_level"`
	Timeout            time.Duration `yaml:"timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout"`
	TimeoutShutdown    time.Duration `yaml:"timeout_shutdown"`
	Concurrency        int           `yaml:"concurrency"`
	Metadata           bool          `yaml:"metadata"`
	LogResponseErrors  bool          `yaml:"log_response_errors"`
	MaxConnDuration    time.Duration `yaml:"max_connection_duration"`
	MaxConnsPerHost    int           `yaml:"max_conns_per_host"`
	MaxRequestBodySize int           `yaml:"max_request_body_size"`

	Auth struct {
		Egress struct {
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		} `yaml:"egress"`
	} `yaml:"auth"`

	Tenant struct {
		Label              string         `yaml:"label"`
		LabelList          []string       `yaml:"label_list"`
		Prefix             string         `yaml:"prefix"`
		PrefixPreferSource bool           `yaml:"prefix_prefer_source"`
		LabelRemove        bool           `yaml:"label_remove"`
		Header             string         `yaml:"header"`
		Default            string         `yaml:"default"`
		AcceptAll          bool           `yaml:"accept_all"`
		LabelValueMatcher  []LabelMatcher `yaml:"label_value_matcher"`
	} `yaml:"tenant"`

	// For testing
	testListener net.Listener
	testClient   *http.Client
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool gets a boolean environment variable with a default value
func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

// getEnvInt gets an integer environment variable with a default value
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

// getEnvDuration gets a duration environment variable with a default value
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

// getEnvStringSlice gets a comma-separated string slice environment variable
func getEnvStringSlice(key string, defaultValue []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	// Split by comma and trim spaces
	parts := strings.Split(value, ",")
	result := make([]string, len(parts))
	for i, part := range parts {
		result[i] = strings.TrimSpace(part)
	}
	return result
}

func configLoad(file string) (*config, error) {
	cfg := &config{}

	// Load from YAML file first
	if file != "" {
		y, err := os.ReadFile(filepath.Clean(file))
		if err != nil {
			return nil, errors.Wrap(err, "Unable to read config")
		}

		if err := yaml.UnmarshalStrict(y, cfg); err != nil {
			return nil, errors.Wrap(err, "Unable to parse config")
		}
	}

	// Override with environment variables
	cfg.Listen = getEnv("PRWPROXY_LISTEN", cfg.Listen)
	cfg.ListenPath = getEnv("PRWPROXY_LISTEN_PATH", cfg.ListenPath)
	cfg.ListenPprof = getEnv("PRWPROXY_LISTEN_PPROF", cfg.ListenPprof)
	cfg.ListenMetricsAddress = getEnv("PRWPROXY_LISTEN_METRICS_ADDRESS", cfg.ListenMetricsAddress)
	cfg.MetricsIncludeTenant = getEnvBool("PRWPROXY_METRICS_INCLUDE_TENANT", cfg.MetricsIncludeTenant)

	cfg.TLSCertFile = getEnv("PRWPROXY_TLS_CERT_FILE", cfg.TLSCertFile)
	cfg.TLSKeyFile = getEnv("PRWPROXY_TLS_KEY_FILE", cfg.TLSKeyFile)

	cfg.Target = getEnv("PRWPROXY_TARGET", cfg.Target)
	cfg.EnableIPv6 = getEnvBool("PRWPROXY_ENABLE_IPV6", cfg.EnableIPv6)

	cfg.EnableHTTP2 = getEnvBool("PRWPROXY_ENABLE_HTTP2", cfg.EnableHTTP2)

	cfg.LogLevel = getEnv("PRWPROXY_LOG_LEVEL", cfg.LogLevel)
	cfg.Timeout = getEnvDuration("PRWPROXY_TIMEOUT", cfg.Timeout)
	cfg.IdleTimeout = getEnvDuration("PRWPROXY_IDLE_TIMEOUT", cfg.IdleTimeout)
	cfg.TimeoutShutdown = getEnvDuration("PRWPROXY_TIMEOUT_SHUTDOWN", cfg.TimeoutShutdown)
	cfg.Concurrency = getEnvInt("PRWPROXY_CONCURRENCY", cfg.Concurrency)
	cfg.Metadata = getEnvBool("PRWPROXY_METADATA", cfg.Metadata)
	cfg.LogResponseErrors = getEnvBool("PRWPROXY_LOG_RESPONSE_ERRORS", cfg.LogResponseErrors)
	cfg.MaxConnDuration = getEnvDuration("PRWPROXY_MAX_CONN_DURATION", cfg.MaxConnDuration)
	cfg.MaxConnsPerHost = getEnvInt("PRWPROXY_MAX_CONNS_PER_HOST", cfg.MaxConnsPerHost)
	cfg.MaxRequestBodySize = getEnvInt("PRWPROXY_MAX_REQUEST_BODY_SIZE", cfg.MaxRequestBodySize)

	// Auth settings
	cfg.Auth.Egress.Username = getEnv("PRWPROXY_AUTH_EGRESS_USERNAME", cfg.Auth.Egress.Username)
	cfg.Auth.Egress.Password = getEnv("PRWPROXY_AUTH_EGRESS_PASSWORD", cfg.Auth.Egress.Password)

	// Tenant settings
	cfg.Tenant.Label = getEnv("PRWPROXY_TENANT_LABEL", cfg.Tenant.Label)
	if labelList := getEnvStringSlice("PRWPROXY_TENANT_LABEL_LIST", nil); labelList != nil {
		cfg.Tenant.LabelList = labelList
	}
	cfg.Tenant.Prefix = getEnv("PRWPROXY_TENANT_PREFIX", cfg.Tenant.Prefix)
	cfg.Tenant.PrefixPreferSource = getEnvBool("PRWPROXY_TENANT_PREFIX_PREFER_SOURCE", cfg.Tenant.PrefixPreferSource)
	cfg.Tenant.LabelRemove = getEnvBool("PRWPROXY_TENANT_LABEL_REMOVE", cfg.Tenant.LabelRemove)
	cfg.Tenant.Header = getEnv("PRWPROXY_TENANT_HEADER", cfg.Tenant.Header)
	cfg.Tenant.Default = getEnv("PRWPROXY_TENANT_DEFAULT", cfg.Tenant.Default)
	cfg.Tenant.AcceptAll = getEnvBool("PRWPROXY_TENANT_ACCEPT_ALL", cfg.Tenant.AcceptAll)

	// Set defaults
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:9090"
	}

	if cfg.ListenPath == "" {
		cfg.ListenPath = "/api/v1/write"
	}

	if cfg.ListenMetricsAddress == "" {
		cfg.ListenMetricsAddress = "127.0.0.1:8081"
	}

	if cfg.LogLevel == "" {
		cfg.LogLevel = "warn"
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 60 * time.Second
	}

	if cfg.Concurrency == 0 {
		cfg.Concurrency = 100
	}

	if cfg.Tenant.Header == "" {
		cfg.Tenant.Header = "X-Scope-OrgID"
	}

	if cfg.Tenant.Label == "" {
		cfg.Tenant.Label = "__tenant__"
	}

	// Default to the Label if list is empty
	if len(cfg.Tenant.LabelList) == 0 {
		cfg.Tenant.LabelList = append(cfg.Tenant.LabelList, cfg.Tenant.Label)
	}

	if cfg.Auth.Egress.Username != "" {
		if cfg.Auth.Egress.Password == "" {
			return nil, fmt.Errorf("egress auth user specified, but the password is not")
		}
	}

	if cfg.MaxConnsPerHost == 0 {
		cfg.MaxConnsPerHost = 64
	}

	// Set default max request body size to 8MB if not configured
	if cfg.MaxRequestBodySize == 0 {
		cfg.MaxRequestBodySize = 8 * 1024 * 1024 // 8MB
	}

	for _, matcher := range cfg.Tenant.LabelValueMatcher {
		regex, err := regexp.Compile(matcher.RegexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid regex for '%s': %v", matcher.Name, err)
		}

		LabelValueRegex = append(LabelValueRegex, CompiledMatcher{
			Name:  matcher.Name,
			Regex: regex,
		})
	}

	return cfg, nil
}
