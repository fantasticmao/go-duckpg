package duckpg

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zeebo/assert"
	"testing"
	"time"
)

func TestPgTypeMap(t *testing.T) {
	data := []any{
		true,
		"hello",
		1,
		int8(1),
		int16(1),
		int32(1),
		int64(1),
		float32(1.0),
		1.0,
		byte(1),
		[]byte{1, 2, 3},
		time.Now(),
		time.Minute,
	}
	typeMap := pgtype.NewMap()
	for i, v := range data {
		dt, ok := typeMap.TypeForValue(v)
		assert.True(t, ok)
		fmt.Printf("[%d] name: %v, oid: %v\n", i, dt.Name, dt.OID)
	}
}
