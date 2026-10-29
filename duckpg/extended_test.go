package duckpg

import (
	"testing"

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

func TestFormatCodeAt(t *testing.T) {
	assert.Equal(t, pgtype.TextFormatCode, formatCodeAt(nil, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{1}, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{1}, 3))
	assert.Equal(t, int16(0), formatCodeAt([]int16{0, 1}, 0))
	assert.Equal(t, int16(1), formatCodeAt([]int16{0, 1}, 1))
	assert.Equal(t, pgtype.TextFormatCode, formatCodeAt([]int16{0, 1}, 2))
}
