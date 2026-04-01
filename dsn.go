package cubrid

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// IsolationLevel represents a CUBRID transaction isolation level.
type IsolationLevel int

const (
	IsolationReadCommitted  IsolationLevel = 4
	IsolationRepeatableRead IsolationLevel = 5
	IsolationSerializable   IsolationLevel = 6
)

const (
	defaultPort           = 33000
	defaultUser           = "dba"
	defaultCharset        = "utf-8"
	defaultConnectTimeout = 30 * time.Second
	defaultIsolationLevel = IsolationReadCommitted
)

// DSN holds the parsed connection parameters for a CUBRID database.
type DSN struct {
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	AutoCommit     bool
	Charset        string
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	IsolationLevel IsolationLevel
	LockTimeout    time.Duration
}

// ParseDSN parses a CUBRID DSN string.
// Format: cubrid://user:password@host:port/dbname?param=value
// The "cubrid://" scheme prefix is optional.
func ParseDSN(dsn string) (DSN, error) {
	if dsn == "" {
		return DSN{}, fmt.Errorf("cubrid: empty DSN")
	}

	// Add scheme if missing so net/url can parse it.
	if !strings.HasPrefix(dsn, "cubrid://") {
		dsn = "cubrid://" + dsn
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return DSN{}, fmt.Errorf("cubrid: invalid DSN: %w", err)
	}

	d := DSN{
		AutoCommit:     true,
		Charset:        defaultCharset,
		ConnectTimeout: defaultConnectTimeout,
		IsolationLevel: defaultIsolationLevel,
	}

	// Host
	d.Host = u.Hostname()
	if d.Host == "" {
		return DSN{}, fmt.Errorf("cubrid: missing host in DSN")
	}

	// Port
	portStr := u.Port()
	if portStr == "" {
		d.Port = defaultPort
	} else {
		d.Port, err = strconv.Atoi(portStr)
		if err != nil {
			return DSN{}, fmt.Errorf("cubrid: invalid port %q: %w", portStr, err)
		}
		if d.Port < 1 || d.Port > 65535 {
			return DSN{}, fmt.Errorf("cubrid: port %d out of range (1-65535)", d.Port)
		}
	}

	// Database
	d.Database = strings.TrimPrefix(u.Path, "/")
	if d.Database == "" {
		return DSN{}, fmt.Errorf("cubrid: missing database in DSN")
	}

	// User and password
	if u.User != nil {
		d.User = u.User.Username()
		d.Password, _ = u.User.Password()
	}
	if d.User == "" {
		d.User = defaultUser
	}

	// Query parameters
	for key, values := range u.Query() {
		if len(values) == 0 {
			continue
		}
		val := values[0]
		switch key {
		case "auto_commit":
			d.AutoCommit, err = strconv.ParseBool(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid auto_commit value %q: %w", val, err)
			}
		case "charset":
			d.Charset = val
		case "connect_timeout":
			d.ConnectTimeout, err = time.ParseDuration(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid connect_timeout %q: %w", val, err)
			}
		case "query_timeout":
			d.QueryTimeout, err = time.ParseDuration(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid query_timeout %q: %w", val, err)
			}
		case "isolation_level":
			d.IsolationLevel, err = parseIsolationLevel(val)
			if err != nil {
				return DSN{}, err
			}
		case "lock_timeout":
			d.LockTimeout, err = time.ParseDuration(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid lock_timeout %q: %w", val, err)
			}
		default:
			return DSN{}, fmt.Errorf("cubrid: unknown DSN parameter %q", key)
		}
	}

	return d, nil
}

// String returns the DSN as a connection string.
func (d DSN) String() string {
	var params []string
	if !d.AutoCommit {
		params = append(params, "auto_commit=false")
	}
	if d.Charset != "" && d.Charset != defaultCharset {
		params = append(params, "charset="+url.QueryEscape(d.Charset))
	}
	if d.ConnectTimeout != defaultConnectTimeout {
		params = append(params, "connect_timeout="+d.ConnectTimeout.String())
	}
	if d.QueryTimeout != 0 {
		params = append(params, "query_timeout="+d.QueryTimeout.String())
	}
	if d.IsolationLevel != defaultIsolationLevel {
		params = append(params, "isolation_level="+isolationLevelString(d.IsolationLevel))
	}
	if d.LockTimeout != 0 {
		params = append(params, "lock_timeout="+d.LockTimeout.String())
	}

	query := ""
	if len(params) > 0 {
		query = "?" + strings.Join(params, "&")
	}

	return fmt.Sprintf("cubrid://%s:%s@%s:%d/%s%s",
		url.User(d.User).String(), d.Password,
		d.Host, d.Port, d.Database, query)
}

func parseIsolationLevel(s string) (IsolationLevel, error) {
	switch strings.ToLower(s) {
	case "read_committed":
		return IsolationReadCommitted, nil
	case "repeatable_read":
		return IsolationRepeatableRead, nil
	case "serializable":
		return IsolationSerializable, nil
	default:
		return 0, fmt.Errorf("cubrid: unknown isolation level %q (valid: read_committed, repeatable_read, serializable)", s)
	}
}

func isolationLevelString(level IsolationLevel) string {
	switch level {
	case IsolationReadCommitted:
		return "read_committed"
	case IsolationRepeatableRead:
		return "repeatable_read"
	case IsolationSerializable:
		return "serializable"
	default:
		return fmt.Sprintf("unknown(%d)", level)
	}
}
