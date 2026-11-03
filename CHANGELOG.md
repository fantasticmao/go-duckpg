# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Config.ServerVersion` selects the PostgreSQL version advertised as `server_version`. An empty value uses `15.0`.
- `version()` appends the version of the linked DuckDB library.

## [1.0.0-alpha.1] - 2026-11-02

### Added

- `Listen` serves the PostgreSQL wire protocol for a caller-supplied DuckDB database.
- Simple Query executes one SQL string and returns rows or a command completion tag.
- Extended query accepts Parse, Bind, Describe, Execute, Close, Sync, and Flush.
- Each socket keeps one DuckDB session across later statements, errors, and transactions.
- Each DuckDB column is advertised as the nearest PostgreSQL type, in text or binary.
- A nil `Users` map accepts every client. A non-nil map requires the startup user to be a key and the cleartext password to match that entry.
- `TLS` completes `SSLRequest` when it is set. A nil `TLS` answers `N`, and every `GSSEncRequest` is declined.
- `CancelRequest` interrupts the query whose process ID and secret match.
- The session stores PostgreSQL settings that DuckDB rejects, and `version()` is rewritten for PostgreSQL clients.

[Unreleased]: https://github.com/fantasticmao/go-duckpg/compare/v1.0.0-alpha.1...HEAD
[1.0.0-alpha.1]: https://github.com/fantasticmao/go-duckpg/releases/tag/v1.0.0-alpha.1
