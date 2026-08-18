# cubrid-go

A pure Go [`database/sql`](https://pkg.go.dev/database/sql) driver for the [CUBRID](https://www.cubrid.org/) database. It implements the CCI (CAS Client Interface) binary protocol directly over TCP, with no CGo or C library dependency.

## Quick start

```go
import (
    "database/sql"

    _ "github.com/search5/cubrid-go"
)

db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb")
```

See [`examples/basic_crud`](examples/basic_crud/main.go) for a full CRUD walkthrough.

## DSN format

```
cubrid://user:password@host:port/dbname?param=value
```

Multiple hosts can be given for HA/load-balanced clusters:

```
cubrid://user:password@host1:port1,host2:port2/dbname?ha=true&lb=round_robin
```

The `cubrid://` scheme prefix is optional.

## Features

- **Pure Go** — implements the CCI wire protocol from scratch, no CGo required
- **`database/sql` compatible** — works with the standard `sql.DB` / `sql.Conn` APIs
- **HA & load balancing** (`ha.go`) — multi-broker failover, round-robin / failover / random distribution, optional read/write splitting
- **Connection pool** (`pool.go`) — pool metrics (active / idle / waiting) with a callback hook
- **Transactions & savepoints** (`tx.go`, `savepoint.go`)
- **LOB support** (`lob.go`)
- **Batch execution** (`batch.go`) — `EXECUTE_BATCH` and prepared-statement batching
- **CUBRID-specific types** — `numeric`, `monetary`, `temporal`/timezone-aware datetime, `json`, collection, and OID types
- **Distributed transactions (XA)** (`xa.go`)
- **Schema introspection** (`schema.go`)

## Examples

| Example | Description |
|---------|-------------|
| [`basic_crud`](examples/basic_crud/main.go) | Create, insert, query, update, delete |
| [`transactions`](examples/transactions/main.go) | Transactions and savepoints |
| [`batch_insert`](examples/batch_insert/main.go) | Batch execution |
| [`lob_handling`](examples/lob_handling/main.go) | Large object (LOB) handling |

## Status

Early-stage (`v0.1.0`). The core driver, DSN parsing, connection pooling, and HA/failover paths have test coverage; see the repository's test files for details.
