package duckpg

import (
	"math/big"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zeebo/assert"
)

func TestEncodeTextValue(t *testing.T) {
	day := time.Date(2024, time.January, 2, 15, 4, 5, 0, time.UTC)
	east := time.FixedZone("CST", 8*60*60)
	timestamptz := time.Date(2024, time.January, 2, 3, 4, 5, 0, east)
	uuid := []byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
	}

	cases := []struct {
		name    string
		data    any
		oid     uint32
		want    string
		wantNil bool
	}{
		{name: "null", data: nil, oid: pgtype.Int4OID, wantNil: true},
		{name: "bool true", data: true, oid: pgtype.BoolOID, want: "t"},
		{name: "bool false", data: false, oid: pgtype.BoolOID, want: "f"},
		{name: "int16", data: int16(1), oid: pgtype.Int2OID, want: "1"},
		{name: "int32", data: int32(2), oid: pgtype.Int4OID, want: "2"},
		{name: "int64", data: int64(3), oid: pgtype.Int8OID, want: "3"},
		{name: "float32", data: float32(1.5), oid: pgtype.Float4OID, want: "1.5"},
		{name: "float64", data: float64(2.5), oid: pgtype.Float8OID, want: "2.5"},
		{name: "string", data: "hello", oid: pgtype.TextOID, want: "hello"},
		{name: "date", data: day, oid: pgtype.DateOID, want: "2024-01-02"},
		{name: "timestamptz", data: timestamptz, oid: pgtype.TimestamptzOID, want: "2024-01-01 19:04:05+00"},
		{
			name: "decimal",
			data: duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(1234)},
			oid:  pgtype.NumericOID,
			want: "12.34",
		},
		{
			name: "uuid",
			data: uuid,
			oid:  pgtype.UUIDOID,
			want: "01234567-89ab-cdef-0123-456789abcdef",
		},
		{name: "bytea", data: []byte{0xde, 0xad}, oid: pgtype.ByteaOID, want: `\xdead`},
		{
			name: "interval",
			data: duckdb.Interval{Months: 14, Days: 3, Micros: 3_661_000_000},
			oid:  pgtype.IntervalOID,
			want: "1 years 2 mons 3 days 1:01:01",
		},
		{
			name: "zero interval",
			data: duckdb.Interval{},
			oid:  pgtype.IntervalOID,
			want: "0:00:00",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeTextValue(tc.data, tc.oid)
			assert.NoError(t, err)
			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tc.want, string(got))
		})
	}
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
