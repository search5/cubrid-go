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
