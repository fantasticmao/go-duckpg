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

| DuckDB Data Type     | PG Data Type                                | PG Table Object ID | PG Column Attribute Number | PG Data Type Object ID | PG Data Type Size | PG Type Modifier | PG Format |
|----------------------|---------------------------------------------|--------------------|----------------------------|------------------------|-------------------|------------------|-----------|
| BIGINT               | bigint / int8                               | 0                  | 0                          | 20                     | 8                 | -1               | 0         |
| BIT                  | bit                                         | 0                  | 0                          | 1560                   | -1                | -1               | 0         |
| BLOB                 | bytea                                       | 0                  | 0                          | 17                     | -1                | -1               | 0         |
| BOOLEAN              | boolean                                     | 0                  | 0                          | 16                     | 1                 | -1               | 0         |
| DATE                 | date                                        | 0                  | 0                          | 1082                   | 4                 | -1               | 0         |
| DECIMAL(prec, scale) | numeric / decimal                           | 0                  | 0                          | 1700                   | -1                | -1               | 0         |
| DOUBLE               | double precision / float8                   | 0                  | 0                          | 701                    | 8                 | -1               | 0         |
| FLOAT                | real / float4                               | 0                  | 0                          | 700                    | 4                 | -1               | 0         |
| HUGEINT              | <span style="color: yellow">bigint</span>   |                    |                            |                        |                   |                  |           |
| INTEGER              | integer / int / int4                        | 0                  | 0                          | 23                     | 4                 | -1               | 0         |
| INTERVAL             | interval                                    | 0                  | 0                          | 1186                   | 16                | -1               | 0         |
| JSON                 | json                                        | 0                  | 0                          | 114                    | -1                | -1               | 0         |
| SMALLINT             | smallint / int2                             | 0                  | 0                          | 21                     | 2                 | -1               | 0         |
| TIMETZ               | time with time zone                         | 0                  | 0                          | 1266                   | 12                | -1               | 0         |
| TIME                 | time                                        | 0                  | 0                          | 1083                   | 8                 | -1               | 0         |
| TIMESTAMPTZ          | timestamp with time zone                    | 0                  | 0                          | 1184                   | 8                 | -1               | 0         |
| TIMESTAMP            | timestamp                                   | 0                  | 0                          | 1114                   | 8                 | -1               | 0         |
| TINYINT              | <span style="color: yellow">smallint</span> |                    |                            |                        |                   |                  |           |
| UBIGINT              | <span style="color: yellow">bigint</span>   |                    |                            |                        |                   |                  |           |
| UHUGEINT             | <span style="color: yellow">bigint</span>   |                    |                            |                        |                   |                  |           |
| UINTEGER             | <span style="color: yellow">integer</span>  |                    |                            |                        |                   |                  |           |
| USMALLINT            | <span style="color: yellow">smallint</span> |                    |                            |                        |                   |                  |           |
| UTINYINT             | <span style="color: yellow">smallint</span> |                    |                            |                        |                   |                  |           |
| UUID                 | uuid                                        | 0                  | 0                          | 2950                   | 16                | -1               | 0         |
| VARCHAR              | text                                        | 0                  | 0                          | 25                     | -1                | -1               | 0         |

