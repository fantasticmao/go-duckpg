# go-duckpg

[![Actions Status](https://github.com/fantasticmao/go-duckpg/workflows/ci/badge.svg)](https://github.com/fantasticmao/go-duckpg/actions)
[![codecov](https://codecov.io/gh/fantasticmao/go-duckpg/branch/main/graph/badge.svg)](https://codecov.io/gh/fantasticmao/go-duckpg)
![Go Version](https://img.shields.io/github/go-mod/go-version/fantasticmao/go-duckpg)
[![License](https://img.shields.io/github/license/fantasticmao/go-duckpg)](LICENSE)

README [English](README.md) | [中文](README_ZH.md)

## What is this

go-duckpg is a Go library that provides the wire protocol of [PostgreSQL](https://www.postgresql.org/) for a caller-supplied [DuckDB](https://duckdb.org/) database, and it does not implement the PostgreSQL SQL dialect, system catalogs, or any authentication method other than a cleartext password. The current library version is [v1.0.0-alpha.1](VERSION).

![usage.png](docs/usage.png)

## Features

- **Simple Query**: executes one SQL string and returns rows or a command completion tag.
- **Extended Query**: accepts Parse, Bind, Describe, Execute, Close, Sync, and Flush.
- **Pooled session**: holds one DuckDB connection from the pool for the life of each socket.
- **Cleartext auth**: checks the startup user and password when `Users` is set, and accepts every client when it is nil.
- **Optional TLS**: completes `SSLRequest` when `TLS` is set, answers `N` when it is nil, and declines `GSSEncRequest`.
- **Query cancel**: interrupts the running statement when `CancelRequest` carries the matching secret.
- **Session compat**: stores settings DuckDB rejects, and rewrites `version()` before the client reads it.
- **Type mapping**: advertises each DuckDB column as the nearest PostgreSQL type, in text or binary.

## Download and Install

```bash
go get github.com/fantasticmao/go-duckpg
```

- **Go toolchain**: building and running the module requires Go 1.26.
- **Native builds**: the DuckDB driver ships `libduckdb` for darwin and linux on amd64 and arm64, and for windows on amd64.

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
	checkError(err)

	err = db.Ping()
	checkError(err)

	err = duckpg.Listen(db, ":15432", duckpg.Config{
		Users:         map[string]string{"duckpg": "123456"},
		TLS:           nil,
		ServerVersion: "15.0",
	})
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
```

Point a PostgreSQL client at that address. The database name in the URL has no effect. A nil `Users` map also ignores the user name.

```bash
psql "postgresql://duckpg:123456@127.0.0.1:15432/duckdb?sslmode=disable"
```

> [!IMPORTANT]
> `Listen` returns when it can no longer accept a connection, and it keeps accepting until that failure.

> [!NOTE]
> The password in the example travels as clear text. Set `Config.TLS` before that port is reachable outside a trusted network, and keep `sslmode=disable` only while `TLS` is nil.

## How it works

go-duckpg separates connection handling, session state, statement compatibility, and value encoding.

### Architecture

- **Wire server**: accepts TCP sockets, finishes startup, and dispatches each frontend message.
- **Client session**: keeps the DuckDB connection, prepared statements, bound portals, transaction status, and the cancel key.
- **Compatibility**: passes a statement through, answers it from session state, or rewrites it before execution.
- **Type codec**: maps each DuckDB column to a PostgreSQL OID and encodes the value as text or binary.

### Core flow

go-duckpg reads startup messages before any SQL. An `SSLRequest` becomes a TLS handshake when `Config.TLS` is set, and both an unconfigured `SSLRequest` and every `GSSEncRequest` receive the byte `N`. A `CancelRequest` stays outside the query loop and cancels the query on the session whose process ID and secret match.

A nil `Config.Users` skips authentication. A non-nil `Config.Users` requests a cleartext password and accepts the connection only when the startup user is a key and the reply matches that entry. go-duckpg then sends `AuthenticationOk`, the parameter statuses, `BackendKeyData`, and `ReadyForQuery`.

A simple `Query` and an extended Parse, Bind, Describe, Execute, and Sync sequence share one classification step. A statement that only changes a stored PostgreSQL setting completes locally. A rewritten statement and every remaining statement run on that session's DuckDB connection. The reply closes with `CommandComplete` and `ReadyForQuery`. After an error in the extended protocol, go-duckpg discards messages until `Sync`, while `Terminate` still closes the session.

```mermaid
flowchart TD
    accept[Accept a TCP socket] --> startup[Read a startup message]
    startup --> kind{Which message}
    kind -->|SSLRequest and TLS is set| tls[Finish the TLS handshake]
    kind -->|SSLRequest and TLS is nil| decline[Send N]
    kind -->|GSSEncRequest| decline
    tls --> startup
    decline --> startup
    kind -->|CancelRequest| cancel[Cancel the matching query]
    kind -->|StartupMessage| users{Users is set}
    users -->|Yes| password[Match the user and cleartext password]
    users -->|No| ready[Send AuthenticationOk and ReadyForQuery]
    password --> ready
    ready --> query[Receive a simple or extended query]
    query --> classify{How the statement is classed}
    classify -->|Stored setting| complete[Send CommandComplete and ReadyForQuery]
    classify -->|Rewrite or pass through| duckdb[Execute on the session connection]
    duckdb --> complete
    complete --> query
```

## FAQ

### Why does a PostgreSQL catalog or dialect query fail?

DuckDB receives the statement, and go-duckpg does not implement `pg_catalog` or the PostgreSQL SQL dialect.

### Why is the database name in the URL ignored?

The startup message supplies the user name, and go-duckpg does not read the database name. Any database name is valid, and `Users` decides which clients may connect.

### Why do SCRAM and MD5 fail?

go-duckpg requests a cleartext password when `Users` is set, and it skips authentication when `Users` is nil. The client accepts a cleartext password, and `Config.TLS` is set before the port is reachable outside a trusted network.

## Changelog

Notable changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

go-duckpg is released under the [MIT License](LICENSE).
