package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const parallelWorkers = 4

// resultRows is the subset of database/sql and pgx row cursors used by the workloads.
type resultRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// Session is one long-lived client session.
type Session interface {
	Exec(ctx context.Context, query string) error
	Query(ctx context.Context, query string) (resultRows, error)
	Close() error
}

// Backend is one access path. Prepare seeds an independent in-memory database.
// Session opens a connection for timed queries. Dial opens a connection whose
// establishment cost is the connect workload.
type Backend interface {
	Name() string
	Prepare(ctx context.Context, rows int) (Checksum, error)
	Session(ctx context.Context) (Session, error)
	Dial(ctx context.Context) (Session, error)
	Close() error
}

// Checksum is the gate compared across access paths before timing starts.
type Checksum struct {
	Count     int64
	SumID     int64
	SumAmount float64
}

func (c Checksum) equal(o Checksum) bool {
	if c.Count != o.Count || c.SumID != o.SumID {
		return false
	}
	return math.Abs(c.SumAmount-o.SumAmount) <= 0.01
}

func (c Checksum) String() string {
	return fmt.Sprintf("count=%d sum_id=%d sum_amount=%g", c.Count, c.SumID, c.SumAmount)
}

type sqlRowSet struct{ *sql.Rows }

func (r sqlRowSet) Close() { _ = r.Rows.Close() }

type sqlSession struct {
	conn    *sql.Conn
	onClose []string
}

func (s *sqlSession) Exec(ctx context.Context, query string) error {
	_, err := s.conn.ExecContext(ctx, query)
	return err
}

func (s *sqlSession) Query(ctx context.Context, query string) (resultRows, error) {
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return sqlRowSet{rows}, nil
}

func (s *sqlSession) Close() error {
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

func openDuckDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
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

const checksumSQL = `SELECT count(*)::BIGINT, coalesce(sum(id), 0)::BIGINT, coalesce(sum(amount), 0)::DOUBLE FROM bench`

func readChecksum(ctx context.Context, s Session, rows int) (Checksum, error) {
	rs, err := s.Query(ctx, checksumSQL)
	if err != nil {
		return Checksum{}, err
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return Checksum{}, err
		}
		return Checksum{}, fmt.Errorf("checksum returned no row")
	}
	var count, sumID, sumAmount any
	if err := rs.Scan(&count, &sumID, &sumAmount); err != nil {
		return Checksum{}, err
	}
	if rs.Next() {
		return Checksum{}, fmt.Errorf("checksum returned more than one row")
	}
	if err := rs.Err(); err != nil {
		return Checksum{}, err
	}
	gotCount, err := toInt64(count)
	if err != nil {
		return Checksum{}, fmt.Errorf("checksum count: %w", err)
	}
	gotID, err := toInt64(sumID)
	if err != nil {
		return Checksum{}, fmt.Errorf("checksum sum(id): %w", err)
	}
	gotAmount, err := toFloat64(sumAmount)
	if err != nil {
		return Checksum{}, fmt.Errorf("checksum sum(amount): %w", err)
	}
	sum := Checksum{Count: gotCount, SumID: gotID, SumAmount: gotAmount}
	if sum.Count != int64(rows) {
		return sum, fmt.Errorf("checksum count %d, want %d", sum.Count, rows)
	}
	wantID := int64(rows) * int64(rows-1) / 2
	if sum.SumID != wantID {
		return sum, fmt.Errorf("checksum sum(id) %d, want %d", sum.SumID, wantID)
	}
	return sum, nil
}

func queryVersion(ctx context.Context, s Session) (string, error) {
	rs, err := s.Query(ctx, `SELECT version()`)
	if err != nil {
		return "", err
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("version() returned no row")
	}
	var v any
	if err := rs.Scan(&v); err != nil {
		return "", err
	}
	return fmt.Sprint(v), rs.Err()
}

func querySelect1(ctx context.Context, s Session) error {
	rs, err := s.Query(ctx, `SELECT 1`)
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

func queryAggregate(ctx context.Context, s Session, rows int) error {
	rs, err := s.Query(ctx, `SELECT count(*), sum(amount) FROM bench`)
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

func queryPoint(ctx context.Context, s Session, id int) error {
	query := fmt.Sprintf("SELECT id, k, s, amount, ts FROM bench WHERE id = %d", id)
	rs, err := s.Query(ctx, query)
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

func queryScan(ctx context.Context, s Session, n int) error {
	query := fmt.Sprintf("SELECT id, k, s, amount, ts FROM bench LIMIT %d", n)
	rs, err := s.Query(ctx, query)
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

func execInsert(ctx context.Context, s Session, id int) error {
	query := fmt.Sprintf("INSERT INTO bench_ins VALUES (%d, 'v')", id)
	return s.Exec(ctx, query)
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
