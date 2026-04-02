package cubrid

import (
	"database/sql"
	"testing"
	"time"
)

func TestNewHAClusterSingleHost(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000/testdb?ha=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	if cluster.Primary() == nil {
		t.Fatal("primary should not be nil")
	}
	// With single host, Standby() returns primary.
	if cluster.Standby() != cluster.Primary() {
		t.Error("standby should equal primary when only one host")
	}
}

func TestNewHAClusterMultiHost(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000/testdb?ha=true&lb=round_robin",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	if cluster.Primary() == nil {
		t.Fatal("primary should not be nil")
	}
	if len(cluster.standbys) != 1 {
		t.Fatalf("expected 1 standby, got %d", len(cluster.standbys))
	}
	if len(cluster.allDBs) != 2 {
		t.Fatalf("expected 2 allDBs, got %d", len(cluster.allDBs))
	}
}

func TestHAClusterDBRouting(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000/testdb?ha=true&rw_split=true&lb=round_robin",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	// Write should always go to primary.
	if cluster.DB(false) != cluster.Primary() {
		t.Error("write should go to primary")
	}

	// Read should go to standby when rw_split is enabled.
	readDB := cluster.DB(true)
	if readDB == cluster.Primary() {
		t.Error("read should go to standby when rw_split is enabled")
	}
}

func TestHAClusterDBRoutingNoSplit(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000/testdb?ha=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	// Without rw_split, reads go to primary.
	if cluster.DB(true) != cluster.Primary() {
		t.Error("without rw_split, reads should go to primary")
	}
}

func TestHAClusterStandbyRoundRobin(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000,192.0.2.3:33000/testdb?ha=true&lb=round_robin",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	if len(cluster.standbys) != 2 {
		t.Fatalf("expected 2 standbys, got %d", len(cluster.standbys))
	}

	// Round-robin should distribute across standbys.
	seen := make(map[*sql.DB]int)
	for i := 0; i < 10; i++ {
		db := cluster.Standby()
		seen[db]++
	}
	if len(seen) != 2 {
		t.Errorf("expected 2 different standbys, got %d", len(seen))
	}
}

func TestHAClusterStandbyFailover(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000,192.0.2.3:33000/testdb?ha=true&lb=failover",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	// Failover mode: always return first standby.
	for i := 0; i < 5; i++ {
		if cluster.Standby() != cluster.standbys[0] {
			t.Error("failover mode should always return first standby")
		}
	}
}

func TestHAClusterCloseIdempotent(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000/testdb?ha=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = cluster.Close()
	_ = cluster.Close() // Should not panic.
}

func TestHAClusterRebuildStandbys(t *testing.T) {
	cluster, err := NewHACluster(HAConfig{
		DSN: "cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000,192.0.2.3:33000/testdb?ha=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	// Simulate failover to index 1.
	cluster.mu.Lock()
	cluster.primaryIdx = 1
	cluster.primary = cluster.allDBs[1]
	cluster.rebuildStandbys()
	cluster.mu.Unlock()

	if len(cluster.standbys) != 2 {
		t.Fatalf("expected 2 standbys after failover, got %d", len(cluster.standbys))
	}
	// Standbys should be allDBs[0] and allDBs[2].
	if cluster.standbys[0] != cluster.allDBs[0] {
		t.Error("first standby should be allDBs[0]")
	}
	if cluster.standbys[1] != cluster.allDBs[2] {
		t.Error("second standby should be allDBs[2]")
	}
}

func TestNewHAConnectorInvalidDSN(t *testing.T) {
	_, err := NewHAConnector("")
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestNewHAConnector(t *testing.T) {
	c, err := NewHAConnector("cubrid://dba:@192.0.2.1:33000,192.0.2.2:33000/testdb?ha=true&lb=round_robin")
	if err != nil {
		t.Fatal(err)
	}
	hc := c.(*haConnector)
	if len(hc.hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hc.hosts))
	}
	if hc.Driver() == nil {
		t.Fatal("driver should not be nil")
	}
}

func TestHAConnectorBrokerOrder(t *testing.T) {
	tests := []struct {
		name string
		lb   LoadBalanceMode
	}{
		{"failover", LBFailover},
		{"round_robin", LBRoundRobin},
		{"random", LBRandom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &haConnector{
				dsn: DSN{
					LoadBalance: tt.lb,
				},
				hosts: []HostPort{
					{Host: "h1", Port: 33000},
					{Host: "h2", Port: 33000},
					{Host: "h3", Port: 33000},
				},
			}

			order := c.brokerOrder()
			if len(order) != 3 {
				t.Fatalf("expected 3 hosts, got %d", len(order))
			}

			if tt.lb == LBFailover {
				// Failover should maintain order.
				if order[0].Host != "h1" {
					t.Error("failover should try first host first")
				}
			}
		})
	}
}

func TestHAConfigDefaults(t *testing.T) {
	c := &HAConfig{}
	if c.failoverInterval() != 10*time.Second {
		t.Errorf("expected 10s failover interval, got %v", c.failoverInterval())
	}
	if c.logger() == nil {
		t.Error("logger should not be nil")
	}
}

// Ensure sql import is used.
var _ *sql.DB
