package cubrid

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPoolEmptyDSN(t *testing.T) {
	_, err := NewPool(PoolConfig{})
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestNewPoolInvalidDSN(t *testing.T) {
	_, err := NewPool(PoolConfig{DSN: "cubrid://localhost:33000/"})
	if err == nil {
		t.Fatal("expected error for invalid DSN (missing database)")
	}
}

func TestPoolMetricsSnapshot(t *testing.T) {
	// Use a DSN that won't actually connect (non-routable IP).
	p, err := NewPool(PoolConfig{
		DSN:                 "cubrid://dba:@192.0.2.1:33000/testdb",
		MaxOpen:             5,
		MaxIdle:             2,
		HealthCheckInterval: -1, // Disable health checks.
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	m := p.Metrics()
	if m.Active != 0 {
		t.Errorf("expected 0 active, got %d", m.Active)
	}
	if m.Idle != 0 {
		t.Errorf("expected 0 idle, got %d", m.Idle)
	}
	if m.Total != 0 {
		t.Errorf("expected 0 total, got %d", m.Total)
	}
}

func TestPoolMetricsCallback(t *testing.T) {
	var called atomic.Int32

	p, err := NewPool(PoolConfig{
		DSN:                 "cubrid://dba:@192.0.2.1:33000/testdb",
		MaxOpen:             5,
		MetricsInterval:     50 * time.Millisecond,
		HealthCheckInterval: -1,
		OnMetrics: func(m PoolMetrics) {
			called.Add(1)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for at least one callback.
	time.Sleep(200 * time.Millisecond)
	p.Close()

	if called.Load() < 1 {
		t.Error("metrics callback was not invoked")
	}
}

func TestPoolDB(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:                 "cubrid://dba:@192.0.2.1:33000/testdb",
		HealthCheckInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if p.DB() == nil {
		t.Fatal("DB() returned nil")
	}
}

func TestPoolCloseIdempotent(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:                 "cubrid://dba:@192.0.2.1:33000/testdb",
		HealthCheckInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Close multiple times should not panic.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close — sql.DB returns error but should not panic.
	_ = p.Close()
}

func TestPoolConfigDefaults(t *testing.T) {
	c := &PoolConfig{}

	if c.healthCheckInterval() != 30*time.Second {
		t.Errorf("expected 30s health check interval, got %v", c.healthCheckInterval())
	}
	if c.metricsInterval() != 15*time.Second {
		t.Errorf("expected 15s metrics interval, got %v", c.metricsInterval())
	}
	if c.logger() == nil {
		t.Error("logger() should not return nil")
	}
}
