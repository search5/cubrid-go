package cubrid

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// PoolMetrics holds snapshot counters for the connection pool.
type PoolMetrics struct {
	Active  int // Connections currently in use.
	Idle    int // Connections idle in the pool.
	Waiting int // Goroutines waiting for a connection.
	Total   int // Total connections (Active + Idle).
}

// MetricsCallback is invoked periodically with pool metrics.
type MetricsCallback func(PoolMetrics)

// PoolConfig configures the CUBRID-aware connection pool.
type PoolConfig struct {
	// DSN is the CUBRID connection string.
	DSN string

	// MaxOpen is the maximum number of open connections. 0 means unlimited.
	MaxOpen int

	// MaxIdle is the maximum number of idle connections. Defaults to 2.
	MaxIdle int

	// MaxLifetime is the maximum time a connection can be reused. 0 means unlimited.
	MaxLifetime time.Duration

	// MaxIdleTime is the maximum time a connection can sit idle. 0 means unlimited.
	MaxIdleTime time.Duration

	// HealthCheckInterval is how often to run health checks on idle connections.
	// Defaults to 30s. Set to 0 to disable periodic health checks.
	HealthCheckInterval time.Duration

	// OnMetrics is called periodically with pool statistics.
	// If nil, metrics are logged via slog at Debug level when Logger is set.
	OnMetrics MetricsCallback

	// MetricsInterval controls how often metrics are collected and reported.
	// Defaults to 15s. Only used when OnMetrics is set or Logger is non-nil.
	MetricsInterval time.Duration

	// Logger for pool events. If nil, uses slog.Default().
	Logger *slog.Logger
}

func (c *PoolConfig) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *PoolConfig) healthCheckInterval() time.Duration {
	if c.HealthCheckInterval != 0 {
		return c.HealthCheckInterval
	}
	return 30 * time.Second
}

func (c *PoolConfig) metricsInterval() time.Duration {
	if c.MetricsInterval != 0 {
		return c.MetricsInterval
	}
	return 15 * time.Second
}

// Pool is a CUBRID-aware connection pool that wraps database/sql.DB
// with health validation, broker failover detection, and observability.
type Pool struct {
	db     *sql.DB
	config PoolConfig

	// Metrics tracking.
	waiting atomic.Int64

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewPool creates a CUBRID-aware connection pool.
func NewPool(config PoolConfig) (*Pool, error) {
	if config.DSN == "" {
		return nil, fmt.Errorf("cubrid: pool DSN is required")
	}

	db, err := sql.Open("cubrid", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("cubrid: pool open: %w", err)
	}

	if config.MaxOpen > 0 {
		db.SetMaxOpenConns(config.MaxOpen)
	}
	if config.MaxIdle > 0 {
		db.SetMaxIdleConns(config.MaxIdle)
	} else {
		db.SetMaxIdleConns(2)
	}
	if config.MaxLifetime > 0 {
		db.SetConnMaxLifetime(config.MaxLifetime)
	}
	if config.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.MaxIdleTime)
	}

	p := &Pool{
		db:     db,
		config: config,
		stopCh: make(chan struct{}),
	}

	// Start background health checker.
	if config.HealthCheckInterval != -1 {
		go p.healthCheckLoop()
	}

	// Start metrics reporter if callback or logger is configured.
	if config.OnMetrics != nil {
		go p.metricsLoop()
	}

	return p, nil
}

// DB returns the underlying *sql.DB for use with standard database/sql APIs.
func (p *Pool) DB() *sql.DB {
	return p.db
}

// Metrics returns a snapshot of current pool metrics.
func (p *Pool) Metrics() PoolMetrics {
	stats := p.db.Stats()
	return PoolMetrics{
		Active:  stats.InUse,
		Idle:    stats.Idle,
		Waiting: int(p.waiting.Load()),
		Total:   stats.OpenConnections,
	}
}

// Conn acquires a connection from the pool with context support.
// The returned *sql.Conn must be closed when done.
func (p *Pool) Conn(ctx context.Context) (*sql.Conn, error) {
	p.waiting.Add(1)
	defer p.waiting.Add(-1)

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("cubrid: pool acquire: %w", err)
	}
	return conn, nil
}

// Ping verifies that the pool can establish a healthy connection.
func (p *Pool) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

// Close shuts down the pool and releases all connections.
func (p *Pool) Close() error {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	return p.db.Close()
}

// healthCheckLoop periodically validates idle connections using CUBRID heartbeat.
func (p *Pool) healthCheckLoop() {
	interval := p.config.healthCheckInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.runHealthCheck()
		}
	}
}

// runHealthCheck pings the database to verify connectivity.
// database/sql automatically removes bad connections from the pool
// via the Validator interface (IsValid) and SessionResetter (ResetSession).
// This explicit ping ensures early detection of broker failover or CAS restart.
func (p *Pool) runHealthCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.db.PingContext(ctx); err != nil {
		p.config.logger().Warn("cubrid pool health check failed",
			"error", err,
		)
	}
}

// metricsLoop periodically collects and reports pool metrics.
func (p *Pool) metricsLoop() {
	interval := p.config.metricsInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			m := p.Metrics()
			if p.config.OnMetrics != nil {
				p.config.OnMetrics(m)
			} else {
				p.config.logger().Debug("cubrid pool metrics",
					"active", m.Active,
					"idle", m.Idle,
					"waiting", m.Waiting,
					"total", m.Total,
				)
			}
		}
	}
}
