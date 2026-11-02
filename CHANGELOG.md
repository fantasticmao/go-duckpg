# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-alpha.1] - 2026-11-02

### Added

- PostgreSQL wire service for a caller-supplied DuckDB database, through `Startup` and `Listen`.
- Simple Query, which executes one SQL string and returns rows or a command completion tag.
- Extended query, which accepts Parse, Bind, Describe, Execute, Close, Sync, and Flush.
- One DuckDB session per connection, kept across SQL errors and transactions.
- PostgreSQL type advertisement for each DuckDB column, in text or supported binary.
- Cleartext password authentication when a password is set, and acceptance of every client when it is empty.
- TLS handshake for `SSLRequest` when `TLS` is set, the byte `N` when it is nil, and refusal of `GSSEncRequest`.
- Query cancellation when `CancelRequest` carries the matching process ID and secret.
- Session storage for PostgreSQL settings that DuckDB 2.0 rejects, and a rewrite of `version()` to `PostgreSQL 15.0 (DuckDB 2.0.0)`.

[Unreleased]: https://github.com/fantasticmao/go-duckpg/compare/v1.0.0-alpha.1...HEAD
[1.0.0-alpha.1]: https://github.com/fantasticmao/go-duckpg/releases/tag/v1.0.0-alpha.1
