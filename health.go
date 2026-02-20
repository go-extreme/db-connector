package dbconnector

import (
	"context"
	"time"
)

type HealthStatus struct {
	Healthy      bool
	ReadLatency  time.Duration
	WriteLatency time.Duration
	Error        error
}

type HealthChecker interface {
	Check(ctx context.Context) *HealthStatus
}

type ConnectorHealthChecker struct {
	connector Connector
}

func NewHealthChecker(connector Connector) *ConnectorHealthChecker {
	return &ConnectorHealthChecker{connector: connector}
}

func (h *ConnectorHealthChecker) Check(ctx context.Context) *HealthStatus {
	status := &HealthStatus{Healthy: true}

	start := time.Now()
	if err := h.connector.Read().DB().PingContext(ctx); err != nil {
		status.Healthy = false
		status.Error = err
	}
	status.ReadLatency = time.Since(start)

	start = time.Now()
	if err := h.connector.Write().DB().PingContext(ctx); err != nil {
		status.Healthy = false
		status.Error = err
	}
	status.WriteLatency = time.Since(start)

	return status
}
