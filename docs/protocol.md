# CCI Binary Protocol Specification

This document describes the CUBRID CCI (CAS Client Interface) binary protocol as implemented by the Go driver. All multi-byte integers are big-endian (network byte order).

## Connection Handshake

The connection is established in two phases over TCP.

### Phase 1: Broker Port Negotiation

The client sends a **Client Info Exchange** packet to the broker port (default 33000):

```
[5 bytes]  "CUBRI" magic
[1 byte]   "D"
[1 byte]   client type (3 = JDBC-compatible)
[1 byte]   reserved (0x00)
[4 bytes]  protocol version (big-endian, e.g. 12)
[1 byte]   SSL flag (0x00 = off, 0x01 = on)
[50 bytes] zero-padded reserved
```

The broker responds with a 4-byte big-endian integer:

| Value | Meaning |
|-------|---------|
| > 0   | New CAS port — close connection and reconnect to this port |
| 0     | Reuse current connection (CAS assigned on same port) |
| < 0   | Connection refused (error code) |

### Phase 2: Database Authentication

After reaching the CAS process, the client sends an **Open Database** request:

```
[32 bytes] database name (zero-padded)
[32 bytes] username (zero-padded)
[32 bytes] password (zero-padded)
[20 bytes] URL string (zero-padded, unused by driver)
[32 bytes] DB password (zero-padded, same as password)
```

The response uses **non-standard framing**:
- `response_length` INCLUDES the 4-byte CAS info (unlike standard messages)
- No separate `response_code` field; body starts with `cas_pid`
- Error detected by checking if `cas_pid` is negative

Response body:
```
[4 bytes]  cas_pid (< 0 = error)
[8 bytes]  broker_info (DBMS type, keep_connection, statement_pooling,
           CCI permanent, protocol_version, function_flags, reserved x2)
[20 bytes] session_id (PROTOCOL_V3+)
```

## Standard Message Framing

### Request Format

```
[4 bytes] payload_length — size of func_code(1) + payload, EXCLUDING CAS info
[4 bytes] CAS info (echoed from server, NEVER modified)
[1 byte]  function code
[N bytes] payload (function-specific parameters)
```

### Response Format

```
[4 bytes] payload_length — size of payload, EXCLUDING CAS info
[4 bytes] CAS info (updated session state)
[4 bytes] response_code (>= 0 success, < 0 error)
[N bytes] body (function-specific data)
```

### CAS Info (4 bytes)

Echoed between every request/response. The client must never modify the autocommit bit.

| Byte | Bits | Purpose |
|------|------|---------|
| 0    | 0x01 | Autocommit flag |
| 0    | 0x02 | Force out-of-transaction |
| 0    | 0x04 | New session ID |
| 1-3  | —    | Reserved (0xFF) |

### Parameter Encoding

Each parameter is length-prefixed:

```
[4 bytes] length of data that follows
[N bytes] data
```

Special cases:
- **Byte parameter**: `[4=1][1-byte value]`
- **Int parameter**: `[4=4][4-byte big-endian int]`
- **Null-terminated string**: `[4=strlen+1][string bytes][0x00]`
- **NULL/reserved**: `[4=0]` (length zero, no data)

## Function Codes

| Code | Name | Purpose |
|------|------|---------|
| 1  | END_TRAN | Commit (op=1) or Rollback (op=2) |
| 2  | PREPARE | Prepare SQL statement |
| 3  | EXECUTE | Execute prepared statement |
| 4  | GET_DB_PARAMETER | Get session parameter value |
| 5  | SET_DB_PARAMETER | Set session parameter value |
| 6  | CLOSE_REQ_HANDLE | Close prepared statement handle |
| 7  | CURSOR | Cursor operations |
| 8  | FETCH | Fetch result rows |
| 9  | SCHEMA_INFO | Query system catalog |
| 10 | OID_GET | Get object attributes by OID |
| 11 | OID_PUT | Update object attributes by OID |
| 15 | GET_DB_VERSION | Get server version string |
| 17 | OID_CMD | OID command operations |
| 18 | COLLECTION | Collection operations |
| 19 | NEXT_RESULT | Advance to next result set |
| 20 | EXECUTE_BATCH | Execute multiple SQL statements |
| 22 | CURSOR_UPDATE | Update row at cursor position |
| 26 | SAVEPOINT | Create (op=1) or rollback to (op=2) savepoint |
| 28 | XA_PREPARE | XA prepare (2PC phase 1) |
| 29 | XA_RECOVER | List in-doubt XA transactions |
| 30 | XA_END_TRAN | XA commit/rollback (2PC phase 2) |
| 31 | CON_CLOSE | Close connection |
| 34 | GET_GENERATED_KEYS | Retrieve auto-generated keys |
| 35 | LOB_NEW | Create new LOB handle |
| 36 | LOB_WRITE | Write data to LOB |
| 37 | LOB_READ | Read data from LOB |
| 38 | END_SESSION | End database session |
| 39 | GET_ROW_COUNT | Get affected row count |
| 40 | GET_LAST_INSERT_ID | Get last auto-generated ID |
| 41 | PREPARE_AND_EXECUTE | Combined prepare + execute |
| 42 | CURSOR_CLOSE | Close server-side cursor |

## PREPARE (Code 2)

### Request

```
[string] SQL query (null-terminated)
[byte]   prepare_flag (0x00 = normal)
[byte]   auto_commit (0x00 or 0x01)
```

### Response

```
[int]    response_code = query_handle
[int]    cache_lifetime
[byte]   statement_type (see Statement Types)
[int]    bind_parameter_count
[byte]   is_updatable
[int]    column_count
[N * column_meta] column metadata
```

### Column Metadata

```
[byte]   type_code (high bit = collection marker for PROTOCOL_V7+)
[byte]   element_type (only if collection marker set)
[short]  scale
[int]    precision
[string] column_name (null-terminated)
[string] real_name (null-terminated)
[string] table_name (null-terminated)
[byte]   nullable
[string] default_value (null-terminated)
[byte]   auto_increment
[byte]   unique_key
[byte]   primary_key
[byte]   reverse_index (ignored)
[byte]   reverse_unique (ignored)
[byte]   foreign_key
[byte]   shared (ignored)
```

## EXECUTE (Code 3)

### Request

```
[int]    query_handle
[byte]   execute_flag (0x00 = normal)
[int]    max_column_size (0 = unlimited)
[int]    max_row_size (0 = unlimited)
[null]   reserved
[byte]   fetch_flag (0x01 for SELECT, 0x00 otherwise)
[byte]   auto_commit
[byte]   forward_only_cursor (0x01)
[int+int] cache_time (seconds + microseconds)
[int]    query_timeout_ms
[N * bind_param] bind parameters
```

### Bind Parameter Encoding

Type and value are SEPARATE length-prefixed fields:

```
[int=1]  type_field_length
[byte]   type_code
[int]    value_length
[N bytes] value_data
```

For NULL:
```
[int=1]  type_field_length
[byte]   NULL type code (0)
[int=0]  value_length (zero)
```

### Response

```
[int]    response_code = total_tuple_count (or affected rows)
[byte]   cache_reusable
[int]    result_count
[N * result_info]:
  [byte]   statement_type
  [int]    affected_count
  [8 bytes] OID
  [int]    cache_time_sec
  [int]    cache_time_usec
[byte]   include_column_info (PROTOCOL_V2+, 0 or 1)
  [if 1: embedded column metadata, same format as PREPARE response]
[int]    shard_id (PROTOCOL_V5+)
[int]    fetch_code (for SELECT, read but ignored)
[int]    inline_tuple_count
[N * tuple] inline tuples (for SELECT with fetch_flag=1)
```

### Tuple Format

```
[int]    row_index (1-based)
[8 bytes] OID
[for each column]:
  [int]    value_length (<=0 = NULL)
  [N bytes] value_data
```

## FETCH (Code 8)

### Request

```
[int]  query_handle
[int]  start_position (1-based)
[int]  fetch_size (number of rows)
[byte] case_sensitive_flag (0x00)
[int]  result_set_index (0 = default)
```

### Response

```
[int]  response_code (< 0 = error, -1012 = no more data)
[int]  tuple_count
[N * tuple] tuples (same format as EXECUTE inline tuples)
```

## CUBRID Data Types

| Code | Type | Wire Format | Go Type |
|------|------|-------------|---------|
| 0  | NULL | — | nil |
| 1  | CHAR | null-terminated string | string |
| 2  | VARCHAR | null-terminated string | string |
| 3  | NCHAR | null-terminated string | string |
| 4  | VARNCHAR | null-terminated string | string |
| 5  | BIT | raw bytes | []byte |
| 6  | VARBIT | raw bytes | []byte |
| 7  | NUMERIC | null-terminated decimal string | *CubridNumeric |
| 8  | INT | 4 bytes big-endian | int32 |
| 9  | SHORT | 2 bytes big-endian | int16 |
| 10 | MONETARY | 8 bytes IEEE 754 double | *CubridMonetary |
| 11 | FLOAT | 4 bytes IEEE 754 | float32 |
| 12 | DOUBLE | 8 bytes IEEE 754 | float64 |
| 13 | DATE | year(2)+month(2)+day(2) | time.Time |
| 14 | TIME | hour(2)+min(2)+sec(2) | time.Time |
| 15 | TIMESTAMP | year(2)+month(2)+day(2)+hour(2)+min(2)+sec(2) | time.Time |
| 16 | SET | collection encoding | *CubridSet |
| 17 | MULTISET | collection encoding | *CubridMultiSet |
| 18 | SEQUENCE | collection encoding | *CubridSequence |
| 19 | OBJECT | 8 bytes (page_id+slot_id+vol_id) | *CubridOid |
| 21 | BIGINT | 8 bytes big-endian | int64 |
| 22 | DATETIME | year(2)+month(2)+day(2)+hour(2)+min(2)+sec(2)+msec(2) | time.Time |
| 23 | BLOB | LOB handle | *CubridLobHandle |
| 24 | CLOB | LOB handle | *CubridLobHandle |
| 25 | ENUM | null-terminated string | *CubridEnum |
| 26 | USHORT | 2 bytes big-endian (PROTOCOL_V6+) | uint16 |
| 27 | UINT | 4 bytes big-endian (PROTOCOL_V6+) | uint32 |
| 28 | UBIGINT | 8 bytes big-endian (PROTOCOL_V6+) | uint64 |
| 29 | TIMESTAMPTZ | 12 bytes + TZ string (PROTOCOL_V7+) | *CubridTimestampTz |
| 30 | TIMESTAMPLTZ | 12 bytes + TZ string (PROTOCOL_V7+) | *CubridTimestampLtz |
| 31 | DATETIMETZ | 14 bytes + TZ string (PROTOCOL_V7+) | *CubridDateTimeTz |
| 32 | DATETIMELTZ | 14 bytes + TZ string (PROTOCOL_V7+) | *CubridDateTimeLtz |
| 34 | JSON | null-terminated string (PROTOCOL_V8+) | *CubridJson |

## Protocol Versions

| Version | Features |
|---------|----------|
| V1  | Query timeout support |
| V2  | Column metadata with execute result |
| V3  | 20-byte session ID |
| V4  | CAS index |
| V5  | Shard feature, fetch end flag |
| V6  | Unsigned integer types (USHORT, UINT, UBIGINT) |
| V7  | Timezone types, collection type encoding in column metadata |
| V8  | JSON type |
| V9  | CAS health check |
| V10 | SSL/TLS support |
| V11 | Out-of-band result set |
| V12 | Trailing zero removal |

## Error Response Format

When `response_code < 0`:

```
[4 bytes] error_indicator
[4 bytes] error_code
[N bytes] error_message (null-terminated)
```

## Autocommit Behavior

- Autocommit is managed **client-side** via a boolean flag
- Each request includes the autocommit flag as a parameter
- CAS info autocommit bit is NEVER modified by the client
- Modifying CAS info causes CAS to close the TCP socket under `KEEP_CONNECTION=AUTO`

## CAS Reconnection

Under `KEEP_CONNECTION=AUTO` (default), CAS may close the socket after any request in autocommit mode. The driver detects EOF/connection-reset and performs:

1. Close existing socket
2. Full two-phase reconnection (broker negotiation + database auth)
3. Retry the failed request (up to 3 attempts)

## LOB Handle Format

```
[4 bytes] LOB type (23=BLOB, 24=CLOB)
[8 bytes] size (int64 big-endian)
[4 bytes] locator_length
[N bytes] locator string
[1 byte]  null terminator
```

## Statement Types

| Code | Type |
|------|------|
| 0  | ALTER TABLE |
| 4  | CREATE TABLE |
| 9  | DROP TABLE |
| 20 | INSERT |
| 21 | SELECT |
| 22 | UPDATE |
| 23 | DELETE |
| 24 | CALL |
| 54 | SELECT UPDATE |
| 57 | MERGE |
| 0x7E | CALL SP |
