//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"
)

const poolTestDSN = "cubrid://dba:@localhost:33000/cubdb"

// --- Pool integration tests ---

func TestIntegrationPoolPing(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:     poolTestDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Ping(ctx); err != nil {
		t.Fatalf("pool ping: %v", err)
	}
}

func TestIntegrationPoolQuery(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:     poolTestDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	db := p.DB()

	var result int
	err = db.QueryRow("SELECT 1 + 1").Scan(&result)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result != 2 {
		t.Fatalf("expected 2, got %d", result)
	}
}

func TestIntegrationPoolConn(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:     poolTestDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := p.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var v int
	err = conn.QueryRowContext(ctx, "SELECT 42").Scan(&v)
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}

func TestIntegrationPoolMetrics(t *testing.T) {
	var collected atomic.Int32

	p, err := NewPool(PoolConfig{
		DSN:             poolTestDSN,
		MaxOpen:         3,
		MaxIdle:         1,
		MetricsInterval: 100 * time.Millisecond,
		OnMetrics: func(m PoolMetrics) {
			collected.Add(1)
			t.Logf("metrics: active=%d idle=%d total=%d waiting=%d",
				m.Active, m.Idle, m.Total, m.Waiting)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a connection to have some metrics.
	if err := p.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}

	time.Sleep(350 * time.Millisecond)
	p.Close()

	if collected.Load() < 1 {
		t.Error("metrics callback was not invoked")
	}
}

func TestIntegrationPoolHealthCheck(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:                 poolTestDSN,
		MaxOpen:             3,
		HealthCheckInterval: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ping to establish a connection.
	if err := p.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Let health check run at least once.
	time.Sleep(500 * time.Millisecond)

	// Pool should still be healthy.
	m := p.Metrics()
	t.Logf("after health check: active=%d idle=%d total=%d", m.Active, m.Idle, m.Total)

	p.Close()
}

func TestIntegrationPoolConcurrent(t *testing.T) {
	p, err := NewPool(PoolConfig{
		DSN:     poolTestDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	db := p.DB()
	const numGoroutines = 10

	errCh := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			var v int
			errCh <- db.QueryRow("SELECT 1").Scan(&v)
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent query: %v", err)
		}
	}

	m := p.Metrics()
	t.Logf("after concurrent: active=%d idle=%d total=%d", m.Active, m.Idle, m.Total)
}

// --- HA integration tests (single broker — validates the plumbing) ---

func TestIntegrationHAClusterSingleBroker(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN:              poolTestDSN + "?ha=true",
		MaxOpenPerBroker: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cluster.Ping(ctx); err != nil {
		t.Fatalf("HA ping: %v", err)
	}

	// Exec on primary.
	_, err = cluster.ExecContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("ExecContext: %v", err)
	}

	// Query.
	rows, err := cluster.QueryContext(ctx, "SELECT 1 + 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var v int
		rows.Scan(&v)
		if v != 3 {
			t.Errorf("expected 3, got %d", v)
		}
	}

	// QueryRow.
	row := cluster.QueryRowContext(ctx, "SELECT 100")
	var v int
	if err := row.Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 100 {
		t.Errorf("expected 100, got %d", v)
	}
}

func TestIntegrationHAConnector(t *testing.T) {
	connector, err := NewHAConnector(poolTestDSN + "?ha=true")
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	var v int
	err = db.QueryRow("SELECT 7 * 6").Scan(&v)
	if err != nil {
		t.Fatalf("query via HAConnector: %v", err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}
