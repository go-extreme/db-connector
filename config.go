package dbconnector

import (
	"fmt"
	"time"
)

type Config struct {
	Host                 string
	Port                 int
	User                 string
	Password             string
	Database             string
	SSLMode              string
	MaxOpenConnection    int
	MaxIdleConnection    int
	ConnMaxLifetime      time.Duration
	ConnMaxIdleTime      time.Duration
	ConnectTimeout       time.Duration // TCP connection timeout (e.g. 10s)
	ApplicationName      string        // shown in pg_stat_activity
	AutoDatabaseCreation bool
}

// WithDefaults returns the config with sensible production defaults applied for
// any zero-value fields.  Call this before Validate() to avoid spurious errors.
func (c *Config) WithDefaults() *Config {
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}
	if c.MaxOpenConnection == 0 {
		c.MaxOpenConnection = 25
	}
	if c.MaxIdleConnection == 0 {
		c.MaxIdleConnection = 5
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 1 * time.Hour
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = 10 * time.Minute
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	return c
}

// Validate checks that required fields are set and values are within acceptable
// ranges.  Returns ErrInvalidConfig (wrapping a descriptive message) on failure.
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("%w: Host is required", ErrInvalidConfig)
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("%w: Port must be between 1 and 65535 (got %d)", ErrInvalidConfig, c.Port)
	}
	if c.User == "" {
		return fmt.Errorf("%w: User is required", ErrInvalidConfig)
	}
	if c.Database == "" {
		return fmt.Errorf("%w: Database is required", ErrInvalidConfig)
	}
	if c.MaxOpenConnection < 0 {
		return fmt.Errorf("%w: MaxOpenConnection must be >= 0", ErrInvalidConfig)
	}
	if c.MaxIdleConnection < 0 {
		return fmt.Errorf("%w: MaxIdleConnection must be >= 0", ErrInvalidConfig)
	}
	if c.MaxIdleConnection > c.MaxOpenConnection && c.MaxOpenConnection > 0 {
		return fmt.Errorf("%w: MaxIdleConnection (%d) cannot exceed MaxOpenConnection (%d)",
			ErrInvalidConfig, c.MaxIdleConnection, c.MaxOpenConnection)
	}
	return nil
}
