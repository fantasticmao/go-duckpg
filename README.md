# go-duckpg

A Go library to connect to [DuckDB](https://duckdb.org/) using [PostgreSQL](https://www.postgresql.org/)'s frontend protocol.

## Supported PostgreSQL Frontend Message

- [x] [StartupMessage](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-STARTUPMESSAGE)
- [x] [Query](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-QUERY)
- [x] [Terminate](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-TERMINATE)
- [ ] [PasswordMessage](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PASSWORDMESSAGE)

## Data Type from DuckDB To PostgreSQL

https://duckdb.org/docs/stable/sql/data_types/overview

https://www.postgresql.org/docs/current/datatype.html

https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION

```sql
SELECT typname, oid, typlen
FROM pg_type
```
