package dbconnector

import "time"

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
	AutoDatabaseCreation bool
}
