package cubrid

import (
	"fmt"
	"net"
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

// LoadBalanceMode controls how the driver distributes connections across brokers.
type LoadBalanceMode string

const (
	// LBRoundRobin distributes connections evenly across all brokers.
	LBRoundRobin LoadBalanceMode = "round_robin"
	// LBFailover always connects to the first available broker.
	LBFailover LoadBalanceMode = "failover"
	// LBRandom picks a random broker for each connection.
	LBRandom LoadBalanceMode = "random"
)

// HostPort represents a single broker endpoint.
type HostPort struct {
	Host string
	Port int
}

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

	// HA / Load Balancing fields.
	// Hosts holds all broker endpoints (including the primary Host:Port).
	// Populated when multiple hosts are specified in the DSN.
	Hosts         []HostPort
	HA            bool            // Enable HA mode.
	LoadBalance   LoadBalanceMode // Load balancing strategy.
	ReadWriteSplit bool           // Route reads to standby, writes to active.
}

// ParseDSN parses a CUBRID DSN string.
// Format: cubrid://user:password@host:port/dbname?param=value
// Multi-host: cubrid://user:password@host1:port1,host2:port2/dbname?ha=true&lb=round_robin
// The "cubrid://" scheme prefix is optional.
func ParseDSN(dsn string) (DSN, error) {
	if dsn == "" {
		return DSN{}, fmt.Errorf("cubrid: empty DSN")
	}

	// Add scheme if missing so net/url can parse it.
	if !strings.HasPrefix(dsn, "cubrid://") {
		dsn = "cubrid://" + dsn
	}

	// Extract multi-host before url.Parse (which doesn't support commas in host).
	hosts, parseable, err := extractMultiHost(dsn)
	if err != nil {
		return DSN{}, err
	}

	u, err := url.Parse(parseable)
	if err != nil {
		return DSN{}, fmt.Errorf("cubrid: invalid DSN: %w", err)
	}

	d := DSN{
		AutoCommit:     true,
		Charset:        defaultCharset,
		ConnectTimeout: defaultConnectTimeout,
		IsolationLevel: defaultIsolationLevel,
		LoadBalance:    LBFailover,
	}

	// Host (first host from the list or the single host).
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

	// Multi-host list.
	if len(hosts) > 1 {
		d.Hosts = hosts
	} else {
		d.Hosts = []HostPort{{Host: d.Host, Port: d.Port}}
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
		case "ha":
			d.HA, err = strconv.ParseBool(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid ha value %q: %w", val, err)
			}
		case "lb":
			switch LoadBalanceMode(val) {
			case LBRoundRobin, LBFailover, LBRandom:
				d.LoadBalance = LoadBalanceMode(val)
			default:
				return DSN{}, fmt.Errorf("cubrid: unknown lb mode %q (valid: round_robin, failover, random)", val)
			}
		case "rw_split":
			d.ReadWriteSplit, err = strconv.ParseBool(val)
			if err != nil {
				return DSN{}, fmt.Errorf("cubrid: invalid rw_split value %q: %w", val, err)
			}
		default:
			return DSN{}, fmt.Errorf("cubrid: unknown DSN parameter %q", key)
		}
	}

	return d, nil
}

// extractMultiHost parses comma-separated hosts from the DSN and returns
// the host list plus a parseable URL (with only the first host for url.Parse).
func extractMultiHost(dsn string) ([]HostPort, string, error) {
	// Find the authority section between "://" and the next "/".
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd < 0 {
		return nil, dsn, nil
	}
	rest := dsn[schemeEnd+3:]

	// Split off userinfo@ if present.
	atIdx := strings.Index(rest, "@")
	pathIdx := strings.Index(rest, "/")

	var hostPart string
	if atIdx >= 0 && (pathIdx < 0 || atIdx < pathIdx) {
		// There is userinfo.
		if pathIdx >= 0 {
			hostPart = rest[atIdx+1 : pathIdx]
		} else {
			hostPart = rest[atIdx+1:]
		}
	} else {
		if pathIdx >= 0 {
			hostPart = rest[:pathIdx]
		} else {
			hostPart = rest
		}
	}

	// Check for comma-separated hosts.
	if !strings.Contains(hostPart, ",") {
		// Single host — parse normally.
		hp, err := parseHostPort(hostPart)
		if err != nil {
			return nil, dsn, err
		}
		return []HostPort{hp}, dsn, nil
	}

	// Multiple hosts.
	parts := strings.Split(hostPart, ",")
	hosts := make([]HostPort, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hp, err := parseHostPort(part)
		if err != nil {
			return nil, "", err
		}
		hosts = append(hosts, hp)
	}

	if len(hosts) == 0 {
		return nil, "", fmt.Errorf("cubrid: no valid hosts in DSN")
	}

	// Rewrite DSN to use only the first host for url.Parse compatibility.
	firstHost := fmt.Sprintf("%s:%d", hosts[0].Host, hosts[0].Port)
	parseable := dsn[:schemeEnd+3] + strings.Replace(rest, hostPart, firstHost, 1)

	return hosts, parseable, nil
}

// parseHostPort parses "host:port" or "host" (defaulting to 33000).
func parseHostPort(s string) (HostPort, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return HostPort{}, fmt.Errorf("cubrid: empty host")
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		// No port specified — use default.
		return HostPort{Host: s, Port: defaultPort}, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return HostPort{}, fmt.Errorf("cubrid: invalid port in %q: %w", s, err)
	}
	if port < 1 || port > 65535 {
		return HostPort{}, fmt.Errorf("cubrid: port %d out of range (1-65535)", port)
	}
	return HostPort{Host: host, Port: port}, nil
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
	if d.HA {
		params = append(params, "ha=true")
	}
	if d.LoadBalance != "" && d.LoadBalance != LBFailover {
		params = append(params, "lb="+string(d.LoadBalance))
	}
	if d.ReadWriteSplit {
		params = append(params, "rw_split=true")
	}

	query := ""
	if len(params) > 0 {
		query = "?" + strings.Join(params, "&")
	}

	// Build host part (multi-host or single).
	hostPart := fmt.Sprintf("%s:%d", d.Host, d.Port)
	if len(d.Hosts) > 1 {
		parts := make([]string, len(d.Hosts))
		for i, hp := range d.Hosts {
			parts[i] = fmt.Sprintf("%s:%d", hp.Host, hp.Port)
		}
		hostPart = strings.Join(parts, ",")
	}

	return fmt.Sprintf("cubrid://%s:%s@%s/%s%s",
		url.User(d.User).String(), d.Password,
		hostPart, d.Database, query)
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
