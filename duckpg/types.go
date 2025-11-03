package duckpg

import (
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"math/big"
	"reflect"
	"time"
)

var goTypeToDuckdbTypeMap = map[reflect.Type]duckdb.Type{
	nil:                               duckdb.TYPE_INVALID,
	reflect.TypeOf(true):              duckdb.TYPE_BOOLEAN,
	reflect.TypeOf(int8(0)):           duckdb.TYPE_TINYINT,
	reflect.TypeOf(int16(0)):          duckdb.TYPE_SMALLINT,
	reflect.TypeOf(int32(0)):          duckdb.TYPE_INTEGER,
	reflect.TypeOf(int64(0)):          duckdb.TYPE_BIGINT,
	reflect.TypeOf(uint8(0)):          duckdb.TYPE_UTINYINT,
	reflect.TypeOf(uint16(0)):         duckdb.TYPE_USMALLINT,
	reflect.TypeOf(uint32(0)):         duckdb.TYPE_UINTEGER,
	reflect.TypeOf(uint64(0)):         duckdb.TYPE_UBIGINT,
	reflect.TypeOf(float32(0)):        duckdb.TYPE_FLOAT,
	reflect.TypeOf(float64(0)):        duckdb.TYPE_DOUBLE,
	reflect.TypeOf(time.Time{}):       duckdb.TYPE_TIMESTAMP_NS,
	reflect.TypeOf(duckdb.Interval{}): duckdb.TYPE_INTERVAL,
	reflect.TypeOf(big.NewInt(0)):     duckdb.TYPE_HUGEINT,
	reflect.TypeOf(""):                duckdb.TYPE_VARCHAR,
	//reflect.TypeOf(""):                duckdb.TYPE_ENUM,
	reflect.TypeOf([]byte{}):         duckdb.TYPE_BLOB,
	reflect.TypeOf(duckdb.Decimal{}): duckdb.TYPE_DECIMAL,
	reflect.TypeOf([]any{}):          duckdb.TYPE_LIST,
	reflect.TypeOf(map[string]any{}): duckdb.TYPE_STRUCT,
	reflect.TypeOf(duckdb.Map{}):     duckdb.TYPE_MAP,
	//reflect.TypeOf([]any{}):           duckdb.TYPE_ARRAY,
	reflect.TypeOf(duckdb.Union{}): duckdb.TYPE_UNION,
	reflect.TypeOf([]byte{}):       duckdb.TYPE_UUID,
}

var duckdbTypeToPgOidMap = map[duckdb.Type]uint32{
	duckdb.TYPE_INVALID:      pgtype.Int2OID,
	duckdb.TYPE_BOOLEAN:      pgtype.Int2OID,
	duckdb.TYPE_TINYINT:      pgtype.Int2OID,
	duckdb.TYPE_SMALLINT:     pgtype.Int2OID,
	duckdb.TYPE_INTEGER:      pgtype.Int2OID,
	duckdb.TYPE_BIGINT:       pgtype.Int8OID,
	duckdb.TYPE_UTINYINT:     pgtype.Int8OID,
	duckdb.TYPE_USMALLINT:    pgtype.Int2OID,
	duckdb.TYPE_UINTEGER:     pgtype.Int2OID,
	duckdb.TYPE_UBIGINT:      pgtype.Int2OID,
	duckdb.TYPE_FLOAT:        pgtype.Int2OID,
	duckdb.TYPE_DOUBLE:       pgtype.Int2OID,
	duckdb.TYPE_TIMESTAMP:    pgtype.Int2OID,
	duckdb.TYPE_DATE:         pgtype.Int2OID,
	duckdb.TYPE_TIME:         pgtype.Int2OID,
	duckdb.TYPE_INTERVAL:     pgtype.Int2OID,
	duckdb.TYPE_HUGEINT:      pgtype.Int2OID,
	duckdb.TYPE_UHUGEINT:     pgtype.Int2OID,
	duckdb.TYPE_VARCHAR:      pgtype.TextOID,
	duckdb.TYPE_BLOB:         pgtype.Int2OID,
	duckdb.TYPE_DECIMAL:      pgtype.Int2OID,
	duckdb.TYPE_TIMESTAMP_S:  pgtype.Int2OID,
	duckdb.TYPE_TIMESTAMP_MS: pgtype.Int2OID,
	duckdb.TYPE_TIMESTAMP_NS: pgtype.Int2OID,
	duckdb.TYPE_ENUM:         pgtype.TextOID,
	duckdb.TYPE_LIST:         pgtype.JSONOID,
	duckdb.TYPE_STRUCT:       pgtype.JSONOID,
	duckdb.TYPE_MAP:          pgtype.JSONOID,
	duckdb.TYPE_ARRAY:        pgtype.Int2OID,
	duckdb.TYPE_UUID:         pgtype.Int2OID,
	duckdb.TYPE_UNION:        pgtype.Int2OID,
	duckdb.TYPE_BIT:          pgtype.Int2OID,
	duckdb.TYPE_TIME_TZ:      pgtype.Int2OID,
	duckdb.TYPE_TIMESTAMP_TZ: pgtype.Int2OID,
	duckdb.TYPE_ANY:          pgtype.Int2OID,
	duckdb.TYPE_BIGNUM:       pgtype.Int2OID,
	duckdb.TYPE_SQLNULL:      pgtype.Int2OID,
}
