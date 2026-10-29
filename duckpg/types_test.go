package duckpg

import (
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zeebo/assert"
)

func TestResolvePgType(t *testing.T) {
	cases := []struct {
		name   string
		oid    uint32
		size   int16
		typmod int32
	}{
		{"BOOLEAN", pgtype.BoolOID, 1, -1},
		{"INTEGER", pgtype.Int4OID, 4, -1},
		{" integer ", pgtype.Int4OID, 4, -1},
		{"BIGINT", pgtype.Int8OID, 8, -1},
		{"VARCHAR", pgtype.TextOID, -1, -1},
		{"DECIMAL(10,2)", pgtype.NumericOID, -1, int32(((10 << 16) | 2) + 4)},
		{"DECIMAL", pgtype.NumericOID, -1, int32(((18 << 16) | 3) + 4)},
		{"TIMESTAMPTZ", pgtype.TimestamptzOID, 8, -1},
		{"UUID", pgtype.UUIDOID, 16, -1},
		{"JSON", pgtype.JSONOID, -1, -1},
		{"UBIGINT", pgtype.NumericOID, -1, -1},
		{"HUGEINT", pgtype.NumericOID, -1, -1},
		{"LIST(INTEGER)", pgtype.JSONOID, -1, -1},
		{"STRUCT(a INTEGER)", pgtype.JSONOID, -1, -1},
		{"MAP(VARCHAR, INTEGER)", pgtype.JSONOID, -1, -1},
		{"INTEGER[]", pgtype.JSONOID, -1, -1},
		{"NOT_A_TYPE", pgtype.TextOID, -1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, typmod := resolvePgType(tc.name)
			assert.Equal(t, tc.oid, oid)
			assert.Equal(t, tc.size, size)
			assert.Equal(t, tc.typmod, typmod)
		})
	}
}

func TestResolvePgTypeFromDuckdbType(t *testing.T) {
	cases := []struct {
		name string
		typ  duckdb.Type
		oid  uint32
		size int16
	}{
		{"BOOLEAN", duckdb.TYPE_BOOLEAN, pgtype.BoolOID, 1},
		{"INTEGER", duckdb.TYPE_INTEGER, pgtype.Int4OID, 4},
		{"VARCHAR", duckdb.TYPE_VARCHAR, pgtype.TextOID, -1},
		{"DECIMAL", duckdb.TYPE_DECIMAL, pgtype.NumericOID, -1},
		{"INVALID", duckdb.TYPE_INVALID, 0, -1},
		{"unknown", duckdb.Type(255), pgtype.TextOID, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, typmod := resolvePgTypeFromDuckdbType(tc.typ)
			assert.Equal(t, tc.oid, oid)
			assert.Equal(t, tc.size, size)
			assert.Equal(t, int32(-1), typmod)
		})
	}
}
