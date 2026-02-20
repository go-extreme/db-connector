package dbconnector

import (
	"context"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

type ConnectionPool struct {
	connections []Connection
	index       uint64
}

func NewConnectionPool(connections ...Connection) *ConnectionPool {
	return &ConnectionPool{connections: connections}
}

func (p *ConnectionPool) DB() *sqlx.DB {
	idx := atomic.AddUint64(&p.index, 1) % uint64(len(p.connections))
	return p.connections[idx].DB()
}

func (p *ConnectionPool) Connect(ctx context.Context) error {
	for _, conn := range p.connections {
		if err := conn.Connect(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *ConnectionPool) Close() error {
	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *ConnectionPool) Connected() bool {
	for _, conn := range p.connections {
		if !conn.Connected() {
			return false
		}
	}
	return true
}

func (p *ConnectionPool) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	return p.connections[0].BeginTx(ctx)
}
