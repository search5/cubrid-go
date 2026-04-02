package cubrid

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// HACluster manages multiple CUBRID broker connections for high availability
// and load balancing. It provides automatic failover, round-robin distribution,
// and optional read/write splitting.
type HACluster struct {
	config  HAConfig
	primary *sql.DB   // Active/write broker.
	standbys []*sql.DB // Standby/read brokers (may be empty).
	allDBs   []*sql.DB // All broker connections in order.

	mu            sync.RWMutex
	primaryIdx    int    // Index of current primary in allDBs.
	rrCounter     atomic.Uint64 // Round-robin counter for read distribution.

	stopOnce sync.Once
	stopCh   chan struct{}
}

// HAConfig configures the HA cluster.
type HAConfig struct {
	// DSN is the CUBRID connection string with multiple hosts.
	// Example: cubrid://dba:@host1:33000,host2:33000/mydb?ha=true&lb=round_robin
	DSN string

	// MaxOpenPerBroker is the max open connections per broker. Defaults to 10.
	MaxOpenPerBroker int

	// MaxIdlePerBroker is the max idle connections per broker. Defaults to 2.
	MaxIdlePerBroker int

	// FailoverCheckInterval controls how often the cluster checks broker health
	// and potentially promotes a standby. Defaults to 10s.
	FailoverCheckInterval time.Duration

	// Logger for HA events. If nil, uses slog.Default().
	Logger *slog.Logger
}

func (c *HAConfig) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *HAConfig) failoverInterval() time.Duration {
	if c.FailoverCheckInterval > 0 {
		return c.FailoverCheckInterval
	}
	return 10 * time.Second
}

// NewHACluster creates an HA-aware cluster from a multi-host DSN.
func NewHACluster(config HAConfig) (*HACluster, error) {
	parsed, err := ParseDSN(config.DSN)
	if err != nil {
		return nil, err
	}

	if len(parsed.Hosts) < 1 {
		return nil, fmt.Errorf("cubrid: HA requires at least one host")
	}

	maxOpen := config.MaxOpenPerBroker
	if maxOpen <= 0 {
		maxOpen = 10
	}
	maxIdle := config.MaxIdlePerBroker
	if maxIdle <= 0 {
		maxIdle = 2
	}

	cluster := &HACluster{
		config: config,
		stopCh: make(chan struct{}),
	}

	// Open a sql.DB for each broker.
	for _, hp := range parsed.Hosts {
		brokerDSN := parsed
		brokerDSN.Host = hp.Host
		brokerDSN.Port = hp.Port
		brokerDSN.Hosts = []HostPort{hp}

		db, err := sql.Open("cubrid", brokerDSN.String())
		if err != nil {
			cluster.closeAll()
			return nil, fmt.Errorf("cubrid: HA open %s:%d: %w", hp.Host, hp.Port, err)
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)

		cluster.allDBs = append(cluster.allDBs, db)
	}

	// First host is the initial primary.
	cluster.primary = cluster.allDBs[0]
	cluster.primaryIdx = 0
	if len(cluster.allDBs) > 1 {
		cluster.standbys = cluster.allDBs[1:]
	}

	// Start failover monitor.
	if parsed.HA {
		go cluster.failoverLoop()
	}

	return cluster, nil
}

// Primary returns the current active/write database connection.
func (c *HACluster) Primary() *sql.DB {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.primary
}

// Standby returns a standby database connection for read queries.
// Selection depends on the load balance mode configured in the DSN.
// If no standbys exist, returns the primary.
func (c *HACluster) Standby() *sql.DB {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.standbys) == 0 {
		return c.primary
	}

	parsed, _ := ParseDSN(c.config.DSN)

	switch parsed.LoadBalance {
	case LBRoundRobin:
		idx := c.rrCounter.Add(1) - 1
		return c.standbys[idx%uint64(len(c.standbys))]
	case LBRandom:
		return c.standbys[rand.Intn(len(c.standbys))]
	default: // LBFailover: always use first standby.
		return c.standbys[0]
	}
}

// DB returns a connection appropriate for the given operation.
// If read/write splitting is enabled, read-only queries go to a standby.
// Otherwise, returns the primary.
func (c *HACluster) DB(readOnly bool) *sql.DB {
	parsed, _ := ParseDSN(c.config.DSN)
	if readOnly && parsed.ReadWriteSplit {
		return c.Standby()
	}
	return c.Primary()
}

// ExecContext executes a write query on the primary broker.
func (c *HACluster) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.Primary().ExecContext(ctx, query, args...)
}

// QueryContext executes a read query. If read/write splitting is enabled,
// it routes to a standby; otherwise, to the primary.
func (c *HACluster) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB(true).QueryContext(ctx, query, args...)
}

// QueryRowContext executes a single-row read query with routing.
func (c *HACluster) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.DB(true).QueryRowContext(ctx, query, args...)
}

// Ping verifies all brokers in the cluster are reachable.
func (c *HACluster) Ping(ctx context.Context) error {
	c.mu.RLock()
	dbs := make([]*sql.DB, len(c.allDBs))
	copy(dbs, c.allDBs)
	c.mu.RUnlock()

	for _, db := range dbs {
		if err := db.PingContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close shuts down all broker connections.
func (c *HACluster) Close() error {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	return c.closeAll()
}

func (c *HACluster) closeAll() error {
	var firstErr error
	for _, db := range c.allDBs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// failoverLoop monitors the primary broker and promotes a standby on failure.
func (c *HACluster) failoverLoop() {
	interval := c.config.failoverInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkAndFailover()
		}
	}
}

// checkAndFailover pings the primary and promotes a standby if it fails.
func (c *HACluster) checkAndFailover() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.mu.RLock()
	primary := c.primary
	primaryIdx := c.primaryIdx
	c.mu.RUnlock()

	if err := primary.PingContext(ctx); err == nil {
		return // Primary is healthy.
	}

	c.config.logger().Warn("cubrid HA: primary broker unreachable, attempting failover",
		"primary_index", primaryIdx,
	)

	// Try each other broker as a candidate.
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, db := range c.allDBs {
		if i == c.primaryIdx {
			continue
		}
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := db.PingContext(checkCtx)
		checkCancel()
		if err == nil {
			// Promote this broker to primary.
			oldIdx := c.primaryIdx
			c.primaryIdx = i
			c.primary = db
			c.rebuildStandbys()

			c.config.logger().Info("cubrid HA: failover complete",
				"old_primary", oldIdx,
				"new_primary", i,
			)
			return
		}
	}

	c.config.logger().Error("cubrid HA: all brokers unreachable, no failover possible")
}

// rebuildStandbys reconstructs the standby list after a primary change. Caller must hold c.mu.
func (c *HACluster) rebuildStandbys() {
	c.standbys = nil
	for i, db := range c.allDBs {
		if i != c.primaryIdx {
			c.standbys = append(c.standbys, db)
		}
	}
}

// --- HA-aware driver.Connector for integration with database/sql ---

// haConnector implements driver.Connector with multi-broker failover.
type haConnector struct {
	dsn    DSN
	driver *CubridDriver
	hosts  []HostPort
	idx    atomic.Uint64
}

// NewHAConnector creates a Connector that tries multiple brokers.
// It can be used with sql.OpenDB for HA without the full HACluster API.
func NewHAConnector(dsnStr string) (driver.Connector, error) {
	dsn, err := ParseDSN(dsnStr)
	if err != nil {
		return nil, err
	}
	return &haConnector{
		dsn:    dsn,
		driver: &CubridDriver{},
		hosts:  dsn.Hosts,
	}, nil
}

// Connect tries brokers in order based on the load balance mode.
func (c *haConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if len(c.hosts) == 0 {
		return nil, fmt.Errorf("cubrid: no hosts configured")
	}

	order := c.brokerOrder()
	var lastErr error

	for _, hp := range order {
		d := c.dsn
		d.Host = hp.Host
		d.Port = hp.Port

		conn, err := connect(ctx, d)
		if err != nil {
			lastErr = err
			continue
		}
		return conn, nil
	}

	return nil, fmt.Errorf("cubrid: all brokers failed: %w", lastErr)
}

// Driver returns the underlying CubridDriver.
func (c *haConnector) Driver() driver.Driver {
	return c.driver
}

// brokerOrder returns the host list in the order to try, based on lb mode.
func (c *haConnector) brokerOrder() []HostPort {
	hosts := make([]HostPort, len(c.hosts))
	copy(hosts, c.hosts)

	switch c.dsn.LoadBalance {
	case LBRoundRobin:
		idx := int(c.idx.Add(1) - 1)
		// Rotate the list so we start at a different host each time.
		n := len(hosts)
		start := idx % n
		rotated := make([]HostPort, n)
		for i := 0; i < n; i++ {
			rotated[i] = hosts[(start+i)%n]
		}
		return rotated
	case LBRandom:
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
		return hosts
	default: // LBFailover: try in order.
		return hosts
	}
}

// Compile-time interface checks.
var _ driver.Connector = (*haConnector)(nil)
