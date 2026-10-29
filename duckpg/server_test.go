package duckpg

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zeebo/assert"
)

func startTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	assert.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				sess, err := newSession(db)
				if err != nil {
					return
				}
				defer sess.close()
				pg := &pgWire{
					conn:    conn,
					backend: pgproto3.NewBackend(conn, conn),
					session: sess,
				}
				_ = pg.run()
			}(conn)
		}
	}()

	cleanup = func() {
		_ = ln.Close()
		<-done
		_ = db.Close()
	}
	return ln.Addr().String(), cleanup
}

func connectPgx(t *testing.T, addr string, simple bool) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://duckpg@%s/duckpg?sslmode=disable", addr))
	assert.NoError(t, err)
	if simple {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestExtendedQuerySelectTypes(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	var (
		b   bool
		i16 int16
		i32 int32
		i64 int64
		f32 float32
		f64 float64
		s   string
		d   time.Time
		n   pgtype.Numeric
	)

	err := conn.QueryRow(ctx, `
		SELECT
			true::BOOLEAN,
			1::SMALLINT,
			2::INTEGER,
			3::BIGINT,
			1.5::FLOAT,
			2.5::DOUBLE,
			'hello'::VARCHAR,
			DATE '2024-01-02',
			DECIMAL '12.34'
	`).Scan(&b, &i16, &i32, &i64, &f32, &f64, &s, &d, &n)
	assert.NoError(t, err)
	assert.Equal(t, true, b)
	assert.Equal(t, int16(1), i16)
	assert.Equal(t, int32(2), i32)
	assert.Equal(t, int64(3), i64)
	assert.Equal(t, float32(1.5), f32)
	assert.Equal(t, 2.5, f64)
	assert.Equal(t, "hello", s)
	assert.Equal(t, 2024, d.Year())
	assert.Equal(t, time.January, d.Month())
	assert.Equal(t, 2, d.Day())
	assert.True(t, n.Valid)
}

func TestExtendedQueryParams(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	var got int32
	err := conn.QueryRow(ctx, `SELECT $1::INTEGER + $2::INTEGER`, 10, 32).Scan(&got)
	assert.NoError(t, err)
	assert.Equal(t, int32(42), got)

	var name string
	err = conn.QueryRow(ctx, `SELECT $1::VARCHAR`, "duckpg").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "duckpg", name)
}

func TestExtendedQueryOIDMapping(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	rows, err := conn.Query(ctx, `SELECT 1::INTEGER AS i, true::BOOLEAN AS b, 'x'::VARCHAR AS s`)
	assert.NoError(t, err)
	defer rows.Close()

	fds := rows.FieldDescriptions()
	assert.Equal(t, 3, len(fds))
	assert.Equal(t, pgtype.Int4OID, fds[0].DataTypeOID)
	assert.Equal(t, pgtype.BoolOID, fds[1].DataTypeOID)
	assert.Equal(t, pgtype.TextOID, fds[2].DataTypeOID)
}

func TestExtendedSQLErrorKeepsConnection(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	_, err := conn.Exec(ctx, `SELECT * FROM definitely_missing_table`)
	assert.Error(t, err)

	var one int
	err = conn.QueryRow(ctx, `SELECT 1`).Scan(&one)
	assert.NoError(t, err)
	assert.Equal(t, 1, one)
}

func TestExtendedTransactionStatus(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	_, err := conn.Exec(ctx, `CREATE TABLE t(id INTEGER)`)
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, `INSERT INTO t VALUES ($1)`, 1)
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, `COMMIT`)
	assert.NoError(t, err)

	var id int
	err = conn.QueryRow(ctx, `SELECT id FROM t`).Scan(&id)
	assert.NoError(t, err)
	assert.Equal(t, 1, id)
}

func TestExtendedQueryExecModes(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	for _, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://duckpg@%s/duckpg?sslmode=disable", addr))
			assert.NoError(t, err)
			cfg.DefaultQueryExecMode = mode

			conn, err := pgx.ConnectConfig(ctx, cfg)
			assert.NoError(t, err)
			defer conn.Close(context.Background())

			var got int32
			err = conn.QueryRow(ctx, `SELECT $1::INTEGER + 1`, 41).Scan(&got)
			assert.NoError(t, err)
			assert.Equal(t, int32(42), got)
		})
	}
}

func TestExtendedNamedPrepare(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	_, err := conn.Prepare(ctx, "add_ints", `SELECT $1::INTEGER + $2::INTEGER`)
	assert.NoError(t, err)

	var sum int32
	err = conn.QueryRow(ctx, "add_ints", 7, 8).Scan(&sum)
	assert.NoError(t, err)
	assert.Equal(t, int32(15), sum)
}

func TestSimpleProtocolRegression(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, true)
	ctx := context.Background()

	var one int
	err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one)
	assert.NoError(t, err)
	assert.Equal(t, 1, one)

	_, err = conn.Exec(ctx, `SELECT * FROM missing`)
	assert.Error(t, err)

	err = conn.QueryRow(ctx, `SELECT 2`).Scan(&one)
	assert.NoError(t, err)
	assert.Equal(t, 2, one)
}

func TestEmptyQuery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, true)
	ctx := context.Background()

	_, err := conn.Exec(ctx, ``)
	assert.NoError(t, err)

	var one int
	err = conn.QueryRow(ctx, `SELECT 1`).Scan(&one)
	assert.NoError(t, err)
	assert.Equal(t, 1, one)
}

func TestResolvePgType(t *testing.T) {
	cases := []struct {
		name string
		oid  uint32
		size int16
	}{
		{"BOOLEAN", pgtype.BoolOID, 1},
		{"INTEGER", pgtype.Int4OID, 4},
		{"BIGINT", pgtype.Int8OID, 8},
		{"VARCHAR", pgtype.TextOID, -1},
		{"DECIMAL(10,2)", pgtype.NumericOID, -1},
		{"TIMESTAMPTZ", pgtype.TimestamptzOID, 8},
		{"UUID", pgtype.UUIDOID, 16},
		{"JSON", pgtype.JSONOID, -1},
		{"UBIGINT", pgtype.NumericOID, -1},
		{"HUGEINT", pgtype.NumericOID, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, _ := resolvePgType(tc.name)
			assert.Equal(t, tc.oid, oid)
			assert.Equal(t, tc.size, size)
		})
	}
}

func TestCommandTag(t *testing.T) {
	assert.Equal(t, "SELECT 3", commandTag("SELECT 1", true, 3, -1))
	assert.Equal(t, "INSERT 0 2", commandTag("INSERT INTO t VALUES (1)", false, 0, 2))
	assert.Equal(t, "BEGIN", commandTag("BEGIN", false, 0, 0))
	assert.Equal(t, "CREATE", commandTag("CREATE TABLE t(i INT)", false, 0, 0))
}
