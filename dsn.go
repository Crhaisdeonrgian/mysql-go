package sql

import (
	"bytes"
	"net/url"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	mysql.Config

	killPoolSize int
	killTimeout  time.Duration
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	var cfg = mysql.NewConfig()

	return &Config{
		Config:       *cfg,
		killPoolSize: defaultKillPoolSize,
		killTimeout:  defaultKillTimeout,
	}
}

func (cfg *Config) Clone() *Config {
	var cp = cfg.Config.Clone()

	return &Config{
		Config:       *cp,
		killPoolSize: cfg.killPoolSize,
		killTimeout:  cfg.killTimeout,
	}
}

func hasParams(dsn string) bool {
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j := i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					return true
				}
			}
			break
		}
	}

	return false
}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}

func (cfg *Config) FormatDSN() string {
	var dsn = cfg.Config.FormatDSN()
	var hasParam = hasParams(dsn)

	var buf bytes.Buffer
	buf.WriteString(dsn)

	if cfg.killPoolSize > 0 {
		writeDSNParam(&buf, &hasParam, "killPoolSize", strconv.Itoa(cfg.killPoolSize))
	}

	if cfg.killTimeout > 0 {
		writeDSNParam(&buf, &hasParam, "killTimeout", cfg.killTimeout.String())
	}

	return buf.String()
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	var mysqlCfg, err = mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	var cfg = Config{
		Config: *mysqlCfg,
	}

	for name, value := range mysqlCfg.Params {
		switch name {
		case "killPoolSize":
			cfg.killPoolSize, err = strconv.Atoi(url.QueryEscape(value))
			if err != nil {
				return nil, err
			}
		// kill queries timeout
		case "killTimeout":
			cfg.killTimeout, err = time.ParseDuration(url.QueryEscape(value))
			if err != nil {
				return nil, err
			}
		}
	}

	if cfg.killPoolSize == 0 {
		cfg.killPoolSize = defaultKillPoolSize
	}

	if cfg.killTimeout == 0 {
		cfg.killTimeout = defaultKillTimeout
	}

	return &cfg, nil
}
