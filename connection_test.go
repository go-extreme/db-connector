package dbconnector

import (
	_ "context"
	"testing"
)

var __TestDBconfig = &Config{
	Host:              "localhost",
	Port:              5432,
	User:              "postgres",
	Password:          "Root1234",
	Database:          "postgres",
	SSLMode:           "disable",
	MaxOpenConnection: 10,
	MaxIdleConnection: 5,
}

var __TestDBreadConfig = &Config{
	Host:              "localhost",
	Port:              5432,
	User:              "postgres",
	Password:          "Root1234",
	Database:          "postgres",
	SSLMode:           "disable",
	MaxOpenConnection: 10,
	MaxIdleConnection: 5,
}

var __TestDBwriteConfig = &Config{
	Host:              "localhost",
	Port:              5432,
	User:              "postgres",
	Password:          "Root1234",
	Database:          "postgres",
	SSLMode:           "disable",
	MaxOpenConnection: 5,
	MaxIdleConnection: 2,
}

func TestPostgresConnection(t *testing.T) {

	conn := NewPostgresConnection(__TestDBconfig)

	if conn.Connected() {
		t.Error("connection should not be established yet")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("DB() should panic when not connected")
		}
	}()
	_ = conn.DB()
}

func TestConnector(t *testing.T) {

	readConn := NewPostgresConnection(__TestDBreadConfig)
	writeConn := NewPostgresConnection(__TestDBwriteConfig)

	connector := NewConnector(readConn, writeConn)

	if connector.Read() != readConn {
		t.Error("Read() should return read connection")
	}

	if connector.Write() != writeConn {
		t.Error("Write() should return write connection")
	}
}
