package duckpg

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
)

func TestClassifyCompat(t *testing.T) {
	s := &session{settings: map[string]string{}}

	t.Run("pass", func(t *testing.T) {
		ordinary := []string{
			`SELECT 1`,
			`SELECT version(), 1`,
			`SET search_path = main`,
			`SELECT nspname FROM pg_catalog.pg_namespace`,
			``,
			`;;;`,
			`SELECT current_setting('no_such_guc')`,
		}
		for _, sql := range ordinary {
			class := classifyCompat(sql, s)
			assert.Equal(t, compatPass, class.kind)
		}
	})

	t.Run("local", func(t *testing.T) {
		cases := []struct {
			sql    string
			tag    string
			setVal string
			reset  string
		}{
			{sql: `SET extra_float_digits = 3`, tag: "SET", setVal: "3"},
			{sql: `SET extra_float_digits TO 3`, tag: "SET", setVal: "3"},
			{sql: `SET extra_float_digits = '3'`, tag: "SET", setVal: "3"},
			{sql: `SET extra_float_digits = 3;`, tag: "SET", setVal: "3"},
			{sql: `SET extra_float_digits = DEFAULT`, tag: "SET", reset: "extra_float_digits"},
			{sql: `RESET extra_float_digits`, tag: "RESET", reset: "extra_float_digits"},
		}
		for _, tc := range cases {
			t.Run(tc.sql, func(t *testing.T) {
				class := classifyCompat(tc.sql, s)
				assert.Equal(t, compatLocal, class.kind)
				assert.Equal(t, tc.tag, class.tag)
				if tc.reset != "" {
					assert.Equal(t, []string{tc.reset}, class.effect.resets)
					return
				}
				assert.Equal(t, tc.setVal, class.effect.sets["extra_float_digits"])
			})
		}
	})

	t.Run("rewrite", func(t *testing.T) {
		cases := []struct {
			sql  string
			want string
		}{
			{
				sql:  `SELECT pg_catalog.version()`,
				want: `SELECT ` + sqlStringLiteral("PostgreSQL 15.0 (DuckDB "+duckdbVersion()+")") + ` AS version`,
			},
			{
				sql:  `SELECT current_setting('server_version')`,
				want: `SELECT '15.0' AS current_setting`,
			},
			{
				sql:  `SHOW transaction_isolation`,
				want: `SELECT 'read committed' AS transaction_isolation`,
			},
			{
				sql:  `SELECT set_config('extra_float_digits', '3', false)`,
				want: `SELECT '3' AS set_config`,
			},
			{
				sql:  `SELECT current_setting('no_such_guc', true)`,
				want: `SELECT CAST(NULL AS VARCHAR) AS current_setting`,
			},
		}
		for _, tc := range cases {
			t.Run(tc.sql, func(t *testing.T) {
				class := classifyCompat(tc.sql, s)
				assert.Equal(t, compatRewrite, class.kind)
				assert.Equal(t, tc.want, class.sql)
			})
		}

		setConfig := classifyCompat(`SELECT set_config('extra_float_digits', '3', false)`, s)
		assert.Equal(t, "3", setConfig.effect.sets["extra_float_digits"])

		custom := &session{settings: map[string]string{"server_version": "16.4"}}
		class := classifyCompat(`SELECT version()`, custom)
		assert.Equal(t, compatRewrite, class.kind)
		assert.Equal(t, `SELECT `+sqlStringLiteral("PostgreSQL 16.4 (DuckDB "+duckdbVersion()+")")+` AS version`, class.sql)
		setting := classifyCompat(`SELECT current_setting('server_version')`, custom)
		assert.Equal(t, compatRewrite, setting.kind)
		assert.Equal(t, `SELECT '16.4' AS current_setting`, setting.sql)
	})
}

func TestApplyCompatEffect(t *testing.T) {
	s := &session{settings: map[string]string{}}

	before := classifyCompat(`SELECT current_setting('extra_float_digits')`, s)
	assert.Equal(t, compatRewrite, before.kind)
	assert.Equal(t, `SELECT '1' AS current_setting`, before.sql)

	set := classifyCompat(`SET extra_float_digits = 3`, s)
	s.applyEffect(set.effect)

	after := classifyCompat(`SELECT current_setting('extra_float_digits')`, s)
	assert.Equal(t, compatRewrite, after.kind)
	assert.Equal(t, `SELECT '3' AS current_setting`, after.sql)
}

func TestCompatStatements(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	conn := connectPgx(t, addr, true)
	ctx := context.Background()

	_, err := conn.Exec(ctx, `SET extra_float_digits = 3`)
	assert.NoError(t, err)

	var digits string
	err = conn.QueryRow(ctx, `SELECT current_setting('extra_float_digits')`).Scan(&digits)
	assert.NoError(t, err)
	assert.Equal(t, "3", digits)

	var version string
	err = conn.QueryRow(ctx, `SELECT version()`).Scan(&version)
	assert.NoError(t, err)
	assert.Equal(t, "PostgreSQL 15.0 (DuckDB "+duckdbVersion()+")", version)

	var serverVersion string
	err = conn.QueryRow(ctx, `SELECT current_setting('server_version')`).Scan(&serverVersion)
	assert.NoError(t, err)
	assert.Equal(t, "15.0", serverVersion)

	var isolation string
	err = conn.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&isolation)
	assert.NoError(t, err)
	assert.Equal(t, "read committed", isolation)
}
