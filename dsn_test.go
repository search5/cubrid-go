package cubrid

import (
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    DSN
		wantErr bool
	}{
		{
			name: "minimal DSN",
			dsn:  "cubrid://dba:@localhost:33000/demodb",
			want: DSN{
				Host:           "localhost",
				Port:           33000,
				Database:       "demodb",
				User:           "dba",
				Password:       "",
				AutoCommit:     true,
				Charset:        "utf-8",
				ConnectTimeout: 30 * time.Second,
				QueryTimeout:   0,
				IsolationLevel: IsolationReadCommitted,
				LockTimeout:    0,
			},
		},
		{
			name: "full DSN with all parameters",
			dsn:  "cubrid://admin:secret@10.0.0.1:33000/mydb?auto_commit=false&charset=euc-kr&connect_timeout=5s&query_timeout=30s&isolation_level=repeatable_read&lock_timeout=10s",
			want: DSN{
				Host:           "10.0.0.1",
				Port:           33000,
				Database:       "mydb",
				User:           "admin",
				Password:       "secret",
				AutoCommit:     false,
				Charset:        "euc-kr",
				ConnectTimeout: 5 * time.Second,
				QueryTimeout:   30 * time.Second,
				IsolationLevel: IsolationRepeatableRead,
				LockTimeout:    10 * time.Second,
			},
		},
		{
			name: "DSN without scheme",
			dsn:  "dba:@localhost:33000/demodb",
			want: DSN{
				Host:           "localhost",
				Port:           33000,
				Database:       "demodb",
				User:           "dba",
				Password:       "",
				AutoCommit:     true,
				Charset:        "utf-8",
				ConnectTimeout: 30 * time.Second,
				IsolationLevel: IsolationReadCommitted,
			},
		},
		{
			name:    "empty DSN",
			dsn:     "",
			wantErr: true,
		},
		{
			name:    "missing database",
			dsn:     "cubrid://dba:@localhost:33000/",
			wantErr: true,
		},
		{
			name:    "missing host",
			dsn:     "cubrid://dba:@:33000/demodb",
			wantErr: true,
		},
		{
			name:    "invalid port",
			dsn:     "cubrid://dba:@localhost:abc/demodb",
			wantErr: true,
		},
		{
			name:    "port out of range",
			dsn:     "cubrid://dba:@localhost:99999/demodb",
			wantErr: true,
		},
		{
			name: "default port when omitted",
			dsn:  "cubrid://dba:@localhost/demodb",
			want: DSN{
				Host:           "localhost",
				Port:           33000,
				Database:       "demodb",
				User:           "dba",
				Password:       "",
				AutoCommit:     true,
				Charset:        "utf-8",
				ConnectTimeout: 30 * time.Second,
				IsolationLevel: IsolationReadCommitted,
			},
		},
		{
			name: "isolation level serializable",
			dsn:  "cubrid://dba:@localhost:33000/demodb?isolation_level=serializable",
			want: DSN{
				Host:           "localhost",
				Port:           33000,
				Database:       "demodb",
				User:           "dba",
				Password:       "",
				AutoCommit:     true,
				Charset:        "utf-8",
				ConnectTimeout: 30 * time.Second,
				IsolationLevel: IsolationSerializable,
			},
		},
		{
			name:    "invalid isolation level",
			dsn:     "cubrid://dba:@localhost:33000/demodb?isolation_level=invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Host != tt.want.Host {
				t.Errorf("Host = %q, want %q", got.Host, tt.want.Host)
			}
			if got.Port != tt.want.Port {
				t.Errorf("Port = %d, want %d", got.Port, tt.want.Port)
			}
			if got.Database != tt.want.Database {
				t.Errorf("Database = %q, want %q", got.Database, tt.want.Database)
			}
			if got.User != tt.want.User {
				t.Errorf("User = %q, want %q", got.User, tt.want.User)
			}
			if got.Password != tt.want.Password {
				t.Errorf("Password = %q, want %q", got.Password, tt.want.Password)
			}
			if got.AutoCommit != tt.want.AutoCommit {
				t.Errorf("AutoCommit = %v, want %v", got.AutoCommit, tt.want.AutoCommit)
			}
			if got.Charset != tt.want.Charset {
				t.Errorf("Charset = %q, want %q", got.Charset, tt.want.Charset)
			}
			if got.ConnectTimeout != tt.want.ConnectTimeout {
				t.Errorf("ConnectTimeout = %v, want %v", got.ConnectTimeout, tt.want.ConnectTimeout)
			}
			if got.QueryTimeout != tt.want.QueryTimeout {
				t.Errorf("QueryTimeout = %v, want %v", got.QueryTimeout, tt.want.QueryTimeout)
			}
			if got.IsolationLevel != tt.want.IsolationLevel {
				t.Errorf("IsolationLevel = %d, want %d", got.IsolationLevel, tt.want.IsolationLevel)
			}
			if got.LockTimeout != tt.want.LockTimeout {
				t.Errorf("LockTimeout = %v, want %v", got.LockTimeout, tt.want.LockTimeout)
			}
		})
	}
}

func TestParseDSNMultiHost(t *testing.T) {
	dsn, err := ParseDSN("cubrid://dba:@host1:33000,host2:33001/mydb?ha=true&lb=round_robin&rw_split=true")
	if err != nil {
		t.Fatal(err)
	}
	if len(dsn.Hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(dsn.Hosts))
	}
	if dsn.Hosts[0].Host != "host1" || dsn.Hosts[0].Port != 33000 {
		t.Errorf("host[0] = %+v", dsn.Hosts[0])
	}
	if dsn.Hosts[1].Host != "host2" || dsn.Hosts[1].Port != 33001 {
		t.Errorf("host[1] = %+v", dsn.Hosts[1])
	}
	if !dsn.HA {
		t.Error("HA should be true")
	}
	if dsn.LoadBalance != LBRoundRobin {
		t.Errorf("LoadBalance = %q, want round_robin", dsn.LoadBalance)
	}
	if !dsn.ReadWriteSplit {
		t.Error("ReadWriteSplit should be true")
	}
	// First host should be primary.
	if dsn.Host != "host1" || dsn.Port != 33000 {
		t.Errorf("primary host = %s:%d", dsn.Host, dsn.Port)
	}
}

func TestParseDSNMultiHostDefaultPort(t *testing.T) {
	dsn, err := ParseDSN("cubrid://dba:@host1,host2/mydb?ha=true")
	if err != nil {
		t.Fatal(err)
	}
	if len(dsn.Hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(dsn.Hosts))
	}
	for i, hp := range dsn.Hosts {
		if hp.Port != 33000 {
			t.Errorf("host[%d] port = %d, want 33000", i, hp.Port)
		}
	}
}

func TestParseDSNInvalidLB(t *testing.T) {
	_, err := ParseDSN("cubrid://dba:@localhost:33000/mydb?lb=invalid")
	if err == nil {
		t.Fatal("expected error for invalid lb mode")
	}
}

func TestParseDSNInvalidHA(t *testing.T) {
	_, err := ParseDSN("cubrid://dba:@localhost:33000/mydb?ha=notbool")
	if err == nil {
		t.Fatal("expected error for invalid ha value")
	}
}

func TestParseDSNInvalidRWSplit(t *testing.T) {
	_, err := ParseDSN("cubrid://dba:@localhost:33000/mydb?rw_split=notbool")
	if err == nil {
		t.Fatal("expected error for invalid rw_split value")
	}
}

func TestDSNStringMultiHost(t *testing.T) {
	dsn := DSN{
		Host:           "host1",
		Port:           33000,
		Database:       "mydb",
		User:           "dba",
		AutoCommit:     true,
		Charset:        "utf-8",
		ConnectTimeout: 30 * time.Second,
		IsolationLevel: IsolationReadCommitted,
		Hosts: []HostPort{
			{Host: "host1", Port: 33000},
			{Host: "host2", Port: 33001},
		},
		HA:             true,
		LoadBalance:    LBRoundRobin,
		ReadWriteSplit: true,
	}

	s := dsn.String()
	// Should contain both hosts.
	if !contains(s, "host1:33000") || !contains(s, "host2:33001") {
		t.Errorf("multi-host DSN string missing hosts: %s", s)
	}
	if !contains(s, "ha=true") {
		t.Errorf("DSN string missing ha=true: %s", s)
	}
	if !contains(s, "lb=round_robin") {
		t.Errorf("DSN string missing lb=round_robin: %s", s)
	}
	if !contains(s, "rw_split=true") {
		t.Errorf("DSN string missing rw_split=true: %s", s)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestDSNString(t *testing.T) {
	dsn := DSN{
		Host:           "localhost",
		Port:           33000,
		Database:       "demodb",
		User:           "dba",
		Password:       "pass",
		AutoCommit:     false,
		Charset:        "utf-8",
		ConnectTimeout: 5 * time.Second,
		QueryTimeout:   30 * time.Second,
		IsolationLevel: IsolationRepeatableRead,
		LockTimeout:    10 * time.Second,
	}

	s := dsn.String()
	roundTrip, err := ParseDSN(s)
	if err != nil {
		t.Fatalf("failed to round-trip DSN: %v", err)
	}
	if roundTrip.Host != dsn.Host || roundTrip.Port != dsn.Port || roundTrip.Database != dsn.Database {
		t.Errorf("round-trip mismatch: got %+v", roundTrip)
	}
}
