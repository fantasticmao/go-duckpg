package duckpg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/zeebo/assert"
)

func TestSimpleQuery(t *testing.T) {
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

	_, err = conn.Exec(ctx, ``)
	assert.NoError(t, err)
}

func TestSimpleQuerySyntaxState(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, true)
	_, err := conn.Exec(context.Background(), `SELEC 1`)
	assert.Error(t, err)

	var pgErr *pgconn.PgError
	assert.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "42601", pgErr.Code)
}

func TestPrefersExec(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"INSERT INTO t VALUES (1)", true},
		{"BEGIN", true},
		{"CREATE TABLE t(i INT)", true},
		{"SELECT 1", false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			assert.Equal(t, tc.want, prefersExec(tc.query))
		})
	}
}
