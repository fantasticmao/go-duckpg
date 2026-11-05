package benchmarks

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// resultRows is the subset of database/sql and pgx row cursors used by the workloads.
type resultRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// session is one long-lived client session.
type session interface {
	exec(ctx context.Context, query string) error
	query(ctx context.Context, query string) (resultRows, error)
	close() error
}

// backend is one access path. session opens a connection for timed queries.
// dial opens a connection whose establishment cost is the connect benchmark.
type backend interface {
	name() string
	session(ctx context.Context) (session, error)
	dial(ctx context.Context) (session, error)
	close() error
}

// checksum is the gate compared across access paths before timing starts.
type checksum struct {
	count     int64
	sumID     int64
	sumAmount float64
}

func (c checksum) equal(o checksum) bool {
	if c.count != o.count || c.sumID != o.sumID {
		return false
	}
	return math.Abs(c.sumAmount-o.sumAmount) <= 0.01
}

func (c checksum) String() string {
	return fmt.Sprintf("count=%d sum_id=%d sum_amount=%g", c.count, c.sumID, c.sumAmount)
}

type sqlRowSet struct{ *sql.Rows }

func (r sqlRowSet) Close() { _ = r.Rows.Close() }

type sqlSession struct {
	conn    *sql.Conn
	onClose []string
}

func (s *sqlSession) exec(ctx context.Context, query string) error {
	_, err := s.conn.ExecContext(ctx, query)
	return err
}

func (s *sqlSession) query(ctx context.Context, query string) (resultRows, error) {
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return sqlRowSet{rows}, nil
}

func (s *sqlSession) close() error {
	var err error
	for _, query := range s.onClose {
		if _, execErr := s.conn.ExecContext(context.Background(), query); execErr != nil && err == nil {
			err = execErr
		}
	}
	if closeErr := s.conn.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// connLimit is the DuckDB pool size. Parallel benchmarks hold one connection
// per GOMAXPROCS goroutine, and -cpu may raise GOMAXPROCS after the pool opens.
func connLimit() int {
	limit := runtime.GOMAXPROCS(0)
	if f := flag.Lookup("test.cpu"); f != nil {
		for _, part := range strings.Split(f.Value.String(), ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err == nil && n > limit {
				limit = n
			}
		}
	}
	if limit < 16 {
		limit = 16
	}
	return limit
}

func openDuckDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	limit := connLimit()
	db.SetMaxOpenConns(limit)
	db.SetMaxIdleConns(limit)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func createBenchSQL(rows int) string {
	return "CREATE TABLE bench AS SELECT " +
		"i::BIGINT AS id, " +
		"(i % 1000)::INTEGER AS k, " +
		"'val-' || i::VARCHAR AS s, " +
		"(i * 1.5)::DOUBLE AS amount, " +
		"TIMESTAMP '2020-01-01' + i * INTERVAL 1 SECOND AS ts " +
		"FROM range(" + strconv.Itoa(rows) + ") t(i)"
}

func seedBench(ctx context.Context, db *sql.DB, rows int) error {
	if _, err := db.ExecContext(ctx, createBenchSQL(rows)); err != nil {
		return fmt.Errorf("create bench: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE bench_ins (id BIGINT, s VARCHAR)`); err != nil {
		return fmt.Errorf("create bench_ins: %w", err)
	}
	return nil
}

const checksumSQL = `SELECT count(*)::BIGINT, coalesce(sum(id), 0)::BIGINT, ` +
	`coalesce(sum(amount), 0)::DOUBLE FROM bench`

func readChecksum(ctx context.Context, s session, rows int) (checksum, error) {
	rs, err := s.query(ctx, checksumSQL)
	if err != nil {
		return checksum{}, err
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return checksum{}, err
		}
		return checksum{}, fmt.Errorf("checksum returned no row")
	}
	var count, sumID, sumAmount any
	if err := rs.Scan(&count, &sumID, &sumAmount); err != nil {
		return checksum{}, err
	}
	if rs.Next() {
		return checksum{}, fmt.Errorf("checksum returned more than one row")
	}
	if err := rs.Err(); err != nil {
		return checksum{}, err
	}
	gotCount, err := toInt64(count)
	if err != nil {
		return checksum{}, fmt.Errorf("checksum count: %w", err)
	}
	gotID, err := toInt64(sumID)
	if err != nil {
		return checksum{}, fmt.Errorf("checksum sum(id): %w", err)
	}
	gotAmount, err := toFloat64(sumAmount)
	if err != nil {
		return checksum{}, fmt.Errorf("checksum sum(amount): %w", err)
	}
	sum := checksum{count: gotCount, sumID: gotID, sumAmount: gotAmount}
	if sum.count != int64(rows) {
		return sum, fmt.Errorf("checksum count %d, want %d", sum.count, rows)
	}
	wantID := int64(rows) * int64(rows-1) / 2
	if sum.sumID != wantID {
		return sum, fmt.Errorf("checksum sum(id) %d, want %d", sum.sumID, wantID)
	}
	return sum, nil
}

func querySelect1(ctx context.Context, s session) error {
	rs, err := s.query(ctx, `SELECT 1`)
	if err != nil {
		return err
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return err
		}
		return fmt.Errorf("SELECT 1 returned no row")
	}
	var v any
	if err := rs.Scan(&v); err != nil {
		return err
	}
	got, err := toInt64(v)
	if err != nil {
		return err
	}
	if got != 1 {
		return fmt.Errorf("SELECT 1 returned %d", got)
	}
	return rs.Err()
}

func queryAggregate(ctx context.Context, s session, rows int) error {
	rs, err := s.query(ctx, `SELECT count(*), sum(amount) FROM bench`)
	if err != nil {
		return err
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return err
		}
		return fmt.Errorf("aggregate returned no row")
	}
	var count, amount any
	if err := rs.Scan(&count, &amount); err != nil {
		return err
	}
	got, err := toInt64(count)
	if err != nil {
		return fmt.Errorf("aggregate count: %w", err)
	}
	if got != int64(rows) {
		return fmt.Errorf("aggregate count %d, want %d", got, rows)
	}
	amt, err := toFloat64(amount)
	if err != nil {
		return fmt.Errorf("aggregate sum: %w", err)
	}
	if rows > 1 && amt <= 0 {
		return fmt.Errorf("aggregate sum %g", amt)
	}
	return rs.Err()
}

func queryPoint(ctx context.Context, s session, id int) error {
	query := fmt.Sprintf("SELECT id, k, s, amount, ts FROM bench WHERE id = %d", id)
	rs, err := s.query(ctx, query)
	if err != nil {
		return err
	}
	defer rs.Close()
	n, got, err := drainBench(rs)
	if err != nil {
		return err
	}
	if n != 1 || got != int64(id) {
		return fmt.Errorf("point id %d returned %d rows, id %d", id, n, got)
	}
	return nil
}

func queryScan(ctx context.Context, s session, n int) error {
	query := fmt.Sprintf("SELECT id, k, s, amount, ts FROM bench LIMIT %d", n)
	rs, err := s.query(ctx, query)
	if err != nil {
		return err
	}
	defer rs.Close()
	got, sum, err := drainBench(rs)
	if err != nil {
		return err
	}
	if got != n {
		return fmt.Errorf("scan returned %d rows, want %d", got, n)
	}
	if n > 1 && sum == 0 {
		return fmt.Errorf("scan id sum is 0")
	}
	return nil
}

func execInsert(ctx context.Context, s session, id int) error {
	query := fmt.Sprintf("INSERT INTO bench_ins VALUES (%d, 'v')", id)
	return s.exec(ctx, query)
}

func drainBench(rs resultRows) (int, int64, error) {
	n := 0
	var sum int64
	for rs.Next() {
		var id, k, s, amount, ts any
		if err := rs.Scan(&id, &k, &s, &amount, &ts); err != nil {
			return n, sum, err
		}
		if id == nil || k == nil || s == nil || amount == nil || ts == nil {
			return n, sum, fmt.Errorf("null benchmark column")
		}
		got, err := toInt64(id)
		if err != nil {
			return n, sum, err
		}
		sum += got
		n++
	}
	if err := rs.Err(); err != nil {
		return n, sum, err
	}
	return n, sum, nil
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case float32:
		return int64(n), nil
	case *big.Int:
		if n == nil {
			return 0, fmt.Errorf("nil big.Int")
		}
		return n.Int64(), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected integer type %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case int32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case *big.Int:
		if n == nil {
			return 0, fmt.Errorf("nil big.Int")
		}
		f, _ := new(big.Float).SetInt(n).Float64()
		return f, nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, fmt.Errorf("unexpected float type %T", v)
	}
}

func failBackend(be backend, err error) (backend, checksum, error) {
	_ = be.close()
	return nil, checksum{}, err
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// waitReady returns when addr accepts a TCP connection, or when errCh receives an error.
// A nil value on errCh means the server call returned and the port may still be coming up.
func waitReady(addr string, errCh <-chan error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var dialErr error
	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		dialErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s: %w", addr, dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
