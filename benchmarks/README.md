# benchmarks

## What is this

The benchmark measures localhost latency for duckdb-go, go-duckpg, and Quack with the standard Go benchmark runner, and it does not change the go-duckpg API. Each result is ns/op for one access path.

## Access paths

- **direct**: duckdb-go reads an in-memory database through one pooled connection.
- **duckpg**: pgx uses the extended query protocol and binary results against `Listen` on `127.0.0.1`.
- **quack**: a DuckDB client attaches to `quack_serve` on `127.0.0.1` and sends each statement with `query`.

> [!NOTE]
> The Quack token is included in connection establishment. Timed statements use an attachment that is already open.

> [!NOTE]
> go-duckpg is started with no `Users` map, so this benchmark does not exercise authentication.

## Workloads

The benchmark loads one `bench` table of `id`, `k`, `s`, `amount`, and `ts`, and an empty `bench_ins` table for inserts. `-rows` sets the table size and defaults to 100000. `-short` uses 1000 rows when `-rows` is left at its default. Scan workloads also use 1000 and 10000 rows when those sizes fit in the table. A scan sub-benchmark name includes that row count.

- **Connect**: open and close one session, reported as ns/op.
- **Select 1**: one round trip for `SELECT 1`, reported as ns/op.
- **Aggregate**: `count(*)` and `sum(amount)` over the table, reported as ns/op.
- **Point read**: one row selected by an `id` literal, reported as ns/op.
- **Table scan**: five columns for one scan size, reported as ns/op.
- **Insert**: one autocommitted single-row insert, reported as ns/op.
- **Parallel**: point reads, then inserts, on `GOMAXPROCS` sessions, as ns/op.

> [!NOTE]
> Parallel ns/op is wall time per operation. `-cpu` sets `GOMAXPROCS` for that pair of benchmarks.

## Run

Run the default comparison:

```bash
go test -bench=. ./benchmarks
```

`make benchmark` runs the same comparison with `-count=1`.

Run a short smoke comparison. `-short` uses 1000 rows when `-rows` is left at its default, and `-benchtime` sets how long each benchmark runs:

```bash
go test -bench=. -short -benchtime=200ms ./benchmarks
```

Pass `-rows` to choose the table size:

```bash
go test -bench=. ./benchmarks -rows 10000
```

`-bench` selects workloads by name, and `-cpu` sets the parallel session count:

```bash
go test -bench=Point ./benchmarks
go test -bench=. -cpu=4 ./benchmarks
```

## How it works

### Architecture

Each path has its own in-memory DuckDB. The paths run one after another.

```mermaid
flowchart LR
  harness[benchmarks]
  harness --> direct["duckdb-go sql.Conn"]
  direct --> dbA[in-memory DuckDB]
  harness --> pgx[pgx session]
  pgx --> duckpg["duckpg.Listen 127.0.0.1"]
  duckpg --> dbB[in-memory DuckDB]
  harness --> qclient["duckdb-go client"]
  qclient --> quack["Quack HTTP 127.0.0.1"]
  quack --> dbC["in-memory DuckDB and quack_serve"]
```

direct keeps one connection from the pool for a sequential workload. Parallel workloads open one connection per `GOMAXPROCS` goroutine before the timer starts. duckpg accepts TCP on an ephemeral port and serves that database with no authentication. Quack installs the `quack` extension, calls `quack_serve` with a fixed token, and gives every query session its own DuckDB client. That client attaches the server and runs statements through the attachment's `query` macro, so the SQL executes on the server. The connect workload instead measures `ATTACH` and `CONNECT`, then discards that session: duckdb-go prepares every later statement, and DuckDB does not keep that prepared statement on a session after `CONNECT`.

### Core flow

The benchmark creates `bench` before the timer starts. It then reads `count(*)`, `sum(id)`, and `sum(amount)` through each path and stops when the results differ. Each benchmark times one operation per iteration, and the runner prints ns/op. A sequential benchmark keeps one session open outside the timer. The connect benchmark includes session close in the measured operation. go-duckpg writes a connection log while workloads run.
