package duckpg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zeebo/assert"
)

func TestDecodeParam(t *testing.T) {
	int4Bin, err := pgTypeMap.Encode(pgtype.Int4OID, pgtype.BinaryFormatCode, int32(42), nil)
	assert.NoError(t, err)

	t.Run("null", func(t *testing.T) {
		got, err := decodeParam(pgtype.Int4OID, pgtype.TextFormatCode, nil)
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("unspecified oid text", func(t *testing.T) {
		got, err := decodeParam(0, pgtype.TextFormatCode, []byte("abc"))
		assert.NoError(t, err)
		assert.Equal(t, "abc", got)
	})

	t.Run("int4 text", func(t *testing.T) {
		got, err := decodeParam(pgtype.Int4OID, pgtype.TextFormatCode, []byte("42"))
		assert.NoError(t, err)
		assert.Equal(t, int32(42), got)
	})

	t.Run("bool text", func(t *testing.T) {
		got, err := decodeParam(pgtype.BoolOID, pgtype.TextFormatCode, []byte("t"))
		assert.NoError(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("varchar text", func(t *testing.T) {
		got, err := decodeParam(pgtype.TextOID, pgtype.TextFormatCode, []byte("duckpg"))
		assert.NoError(t, err)
		assert.Equal(t, "duckpg", got)
	})

	t.Run("int4 binary", func(t *testing.T) {
		got, err := decodeParam(pgtype.Int4OID, pgtype.BinaryFormatCode, int4Bin)
		assert.NoError(t, err)
		assert.Equal(t, int32(42), got)
	})

	t.Run("unspecified oid binary", func(t *testing.T) {
		_, err := decodeParam(0, pgtype.BinaryFormatCode, []byte{0, 0, 0, 1})
		assert.Error(t, err)
	})

	t.Run("unsupported binary oid", func(t *testing.T) {
		_, err := decodeParam(99999, pgtype.BinaryFormatCode, []byte{0})
		assert.Error(t, err)
	})
}

func TestNormalizeDriverValue(t *testing.T) {
	_, err := normalizeDriverValue(map[string]any{"bad": make(chan int)})
	assert.Error(t, err)
}

func TestFormatCodeAt(t *testing.T) {
	assert.Equal(t, pgtype.TextFormatCode, formatCodeAt(nil, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{1}, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{1}, 3))
	assert.Equal(t, int16(0), formatCodeAt([]int16{0, 1}, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{0, 1}, 1))
	assert.Equal(t, pgtype.TextFormatCode, formatCodeAt([]int16{0, 1}, 2))
}

func TestEncodeFormattedValue(t *testing.T) {
	text, err := encodeFormattedValue(int32(42), pgtype.Int4OID, pgtype.TextFormatCode)
	assert.NoError(t, err)
	assert.Equal(t, "42", string(text))

	bin, err := encodeFormattedValue(int32(42), pgtype.Int4OID, pgtype.BinaryFormatCode)
	assert.NoError(t, err)
	var i32 int32
	assert.NoError(t, pgTypeMap.Scan(pgtype.Int4OID, pgtype.BinaryFormatCode, bin, &i32))
	assert.Equal(t, int32(42), i32)

	bin, err = encodeFormattedValue(true, pgtype.BoolOID, pgtype.BinaryFormatCode)
	assert.NoError(t, err)
	var b bool
	assert.NoError(t, pgTypeMap.Scan(pgtype.BoolOID, pgtype.BinaryFormatCode, bin, &b))
	assert.True(t, b)

	null, err := encodeFormattedValue(nil, pgtype.Int4OID, pgtype.TextFormatCode)
	assert.NoError(t, err)
	assert.Nil(t, null)
}

func TestExtendedQuery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, false)
	ctx := context.Background()

	var got int32
	err := conn.QueryRow(ctx, `SELECT $1::INTEGER + $2::INTEGER`, 10, 32).Scan(&got)
	assert.NoError(t, err)
	assert.Equal(t, int32(42), got)

	_, err = conn.Exec(ctx, `SELECT * FROM definitely_missing_table`)
	assert.Error(t, err)

	var one int
	err = conn.QueryRow(ctx, `SELECT 1`).Scan(&one)
	assert.NoError(t, err)
	assert.Equal(t, 1, one)

	_, err = conn.Exec(ctx, `CREATE TABLE t(id INTEGER)`)
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

func TestExtendedQueryExecMode(t *testing.T) {
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

func TestExtendedNamedStatement(t *testing.T) {
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
