# go-duckpg

[![Actions Status](https://github.com/fantasticmao/go-duckpg/workflows/ci/badge.svg)](https://github.com/fantasticmao/go-duckpg/actions)
[![codecov](https://codecov.io/gh/fantasticmao/go-duckpg/branch/main/graph/badge.svg)](https://codecov.io/gh/fantasticmao/go-duckpg)
![Go Version](https://img.shields.io/github/go-mod/go-version/fantasticmao/go-duckpg)
[![License](https://img.shields.io/github/license/fantasticmao/go-duckpg)](LICENSE)

README [English](README.md) | [中文](README_ZH.md)

## What is this

go-duckpg serves a caller-supplied DuckDB database over the PostgreSQL wire protocol, without the PostgreSQL SQL dialect, system catalogs, or any authentication method other than a cleartext password.

The library version is [v1.0.0](VERSION). On the wire, go-duckpg advertises `server_version` as `15.0` and rewrites `version()` to `PostgreSQL 15.0 (DuckDB 2.0.0)`.

## Features

- **Simple Query** — executes one SQL string and returns rows or a command completion tag.
- **Extended Query** — accepts Parse, Bind, Describe, Execute, Close, Sync, and Flush.
- **Pooled session** — holds one DuckDB connection from the pool for the life of each socket.
- **Cleartext auth** — checks a cleartext password when one is set, and accepts every client when it is empty.
- **Optional TLS** — completes `SSLRequest` when `TLS` is set, answers `N` when it is nil, and declines `GSSEncRequest`.
- **Query cancel** — interrupts the running statement when `CancelRequest` carries the matching secret.
- **Session compat** — stores settings DuckDB 2.0 rejects, and rewrites `version()` to `PostgreSQL 15.0 (DuckDB 2.0.0)`.
- **Type mapping** — advertises each DuckDB column as the nearest PostgreSQL type, in text or supported binary.

## Download and Install

```bash
go get github.com/fantasticmao/go-duckpg
```

- **Go toolchain** — building and running the module requires Go 1.26.
- **Native builds** — the DuckDB driver ships `libduckdb` for darwin and linux on amd64 and arm64, and for windows on amd64.

## Quick Start

Open a DuckDB database and pass the handle to `Listen`.

```go
package main

import (
	"database/sql"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/fantasticmao/go-duckpg/duckpg"
)

func main() {
	db, err := sql.Open("duckdb", "test.db")
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}

	err = duckpg.Listen(db, ":15432", duckpg.Config{
		Password: "123456",
		TLS:      nil,
	})
	if err != nil {
		panic(err)
	}
}
```

Point a PostgreSQL client at that address. The user and database names in the URL are ignored.

```bash
psql "postgresql://duckpg:123456@127.0.0.1:15432/duckdb?sslmode=disable"
```

> [!IMPORTANT]
> `Listen` keeps accepting connections until the process exits, and returns when the listener can no longer accept one.

> [!NOTE]
> The password in the example travels as clear text. Set `Config.TLS` before that port is reachable outside a trusted network, and keep `sslmode=disable` only while `TLS` is nil.

## How it works

go-duckpg separates connection handling, session state, statement compatibility, and value encoding.

### Architecture

- **Wire server** — accepts TCP sockets, finishes startup, and dispatches each frontend message.
- **Client session** — keeps the DuckDB connection, prepared statements, bound portals, transaction status, and cancel key.
- **Compatibility** — passes a statement through, answers it from session state, or rewrites it before execution.
- **Type codec** — maps each DuckDB column to a PostgreSQL OID and encodes the value as text or binary.

### Core flow

go-duckpg reads startup messages before any SQL. An `SSLRequest` becomes a TLS handshake when `Config.TLS` is set, and both an unconfigured `SSLRequest` and every `GSSEncRequest` receive the byte `N`. A `CancelRequest` stays out of the query loop and cancels the query on the session whose process ID and secret match.

A non-empty `Config.Password` causes go-duckpg to request `AuthenticationCleartextPassword` and compare the reply with the configured secret. go-duckpg then sends `AuthenticationOk`, the fixed parameter statuses, `BackendKeyData`, and `ReadyForQuery`.

A simple `Query` and an extended Parse, Bind, Describe, Execute, and Sync sequence share one classification step. A statement that only changes a stored PostgreSQL setting completes locally. A rewritten statement, including `version()`, and every remaining statement run on that session's DuckDB connection. The reply closes with `CommandComplete` and `ReadyForQuery`. After an error in the extended protocol, go-duckpg discards messages until `Sync`, while `Terminate` still closes the session.

```mermaid
flowchart TD
    accept[Accept TCP] --> readMsg[Read startup message]
    readMsg --> kind{Message kind}
    kind -->|SSLRequest with TLS| handshake[Complete TLS handshake]
    kind -->|SSLRequest without TLS| replyN[Reply N]
    kind -->|GSSEncRequest| replyN
    handshake --> readMsg
    replyN --> readMsg
    kind -->|CancelRequest| cancel[Cancel the matching query]
    kind -->|StartupMessage| auth{Password set}
    auth -->|Yes| password[Require cleartext password]
    auth -->|No| ready[AuthenticationOk and ReadyForQuery]
    password --> ready
    ready --> query[Simple Query or extended query]
    query --> classify{Compatibility class}
    classify -->|Local setting| done[CommandComplete and ReadyForQuery]
    classify -->|Rewrite or pass through| exec[Run SQL on the session connection]
    exec --> done
    done --> query
```

## FAQ

### Why does version() report PostgreSQL 15.0?

PostgreSQL clients expect a PostgreSQL version string, which DuckDB does not return. go-duckpg rewrites `SELECT version()` to `PostgreSQL 15.0 (DuckDB 2.0.0)`, while the library version remains the contents of [VERSION](VERSION).

### Why does a connection succeed with any user name?

go-duckpg does not read the user or database field of the startup message. Leave `Password` empty to accept every client, or set it so that only the cleartext secret is compared.

### Why does SET extra_float_digits succeed?

DuckDB 2.0 rejects several PostgreSQL parameters that clients send at connect time. go-duckpg stores `extra_float_digits`, `application_name`, `client_encoding`, `standard_conforming_strings`, `DateStyle`, `TimeZone`, `IntervalStyle`, and `bytea_output` on the session, and forwards every other setting, such as `search_path`, to DuckDB.

## License

go-duckpg is released under the [MIT License](LICENSE).
