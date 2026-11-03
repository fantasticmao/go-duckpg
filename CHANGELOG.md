# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-beta.1] - 2026-11-03

### Added

- `Listen` serves Simple Query and extended query for a caller-supplied DuckDB database.
- Each socket keeps one DuckDB session and advertises each column as the nearest PostgreSQL type.
- A nil `Users` map accepts every client. Otherwise the startup user and cleartext password must match.
- `TLS` completes `SSLRequest` when set and answers `N` when nil. `CancelRequest` interrupts the matching query.
- The session stores settings DuckDB rejects. `server_version` defaults to `15.0`, and `version()` appends the linked DuckDB version.

[Unreleased]: https://github.com/fantasticmao/go-duckpg/compare/v1.0.0-beta.1...HEAD
[1.0.0-beta.1]: https://github.com/fantasticmao/go-duckpg/releases/tag/v1.0.0-beta.1
