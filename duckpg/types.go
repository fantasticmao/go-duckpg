package duckpg

import (
	"strconv"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgTypeMeta describes how a DuckDB column is advertised to PostgreSQL clients.
type pgTypeMeta struct {
	OID  uint32
	Size int16 // typlen; -1 for variable-length
}

// duckdbTypeNameToPgMeta maps DuckDB DatabaseTypeName() base names to PG OID and typlen.
// Types without an exact PG counterpart use the nearest compatible type:
// smaller unsigned ints widen into a signed int that can hold their range;
// UBIGINT / HUGEINT / UHUGEINT / BIGNUM use numeric (may exceed int8).
var duckdbTypeNameToPgMeta = map[string]pgTypeMeta{
	"BOOLEAN":      {OID: pgtype.BoolOID, Size: 1},
	"TINYINT":      {OID: pgtype.Int2OID, Size: 2},
	"SMALLINT":     {OID: pgtype.Int2OID, Size: 2},
	"INTEGER":      {OID: pgtype.Int4OID, Size: 4},
	"BIGINT":       {OID: pgtype.Int8OID, Size: 8},
	"UTINYINT":     {OID: pgtype.Int2OID, Size: 2},
	"USMALLINT":    {OID: pgtype.Int4OID, Size: 4},
	"UINTEGER":     {OID: pgtype.Int8OID, Size: 8},
	"UBIGINT":      {OID: pgtype.NumericOID, Size: -1}, // may exceed int8
	"FLOAT":        {OID: pgtype.Float4OID, Size: 4},
	"DOUBLE":       {OID: pgtype.Float8OID, Size: 8},
	"TIMESTAMP":    {OID: pgtype.TimestampOID, Size: 8},
	"TIMESTAMP_S":  {OID: pgtype.TimestampOID, Size: 8},
	"TIMESTAMP_MS": {OID: pgtype.TimestampOID, Size: 8},
	"TIMESTAMP_NS": {OID: pgtype.TimestampOID, Size: 8},
	"DATE":         {OID: pgtype.DateOID, Size: 4},
	"TIME":         {OID: pgtype.TimeOID, Size: 8},
	"TIMETZ":       {OID: pgtype.TimetzOID, Size: 12},
	"TIMESTAMPTZ":  {OID: pgtype.TimestamptzOID, Size: 8},
	"INTERVAL":     {OID: pgtype.IntervalOID, Size: 16},
	"HUGEINT":      {OID: pgtype.NumericOID, Size: -1},
	"UHUGEINT":     {OID: pgtype.NumericOID, Size: -1},
	"VARCHAR":      {OID: pgtype.TextOID, Size: -1},
	"BLOB":         {OID: pgtype.ByteaOID, Size: -1},
	"BIT":          {OID: pgtype.BitOID, Size: -1},
	"UUID":         {OID: pgtype.UUIDOID, Size: 16},
	"JSON":         {OID: pgtype.JSONOID, Size: -1},
	"ENUM":         {OID: pgtype.TextOID, Size: -1},
	"BIGNUM":       {OID: pgtype.NumericOID, Size: -1},
	"GEOMETRY":     {OID: pgtype.TextOID, Size: -1},
	"VARIANT":      {OID: pgtype.JSONOID, Size: -1},
	"SQLNULL":      {OID: pgtype.TextOID, Size: -1},
	"ANY":          {OID: pgtype.TextOID, Size: -1},
}

const pgVarHdrSize = 4

// duckdbTypeToPgMeta maps duckdb.Type enum values to PostgreSQL OID and typlen.
var duckdbTypeToPgMeta = map[duckdb.Type]pgTypeMeta{
	duckdb.TYPE_BOOLEAN:      {OID: pgtype.BoolOID, Size: 1},
	duckdb.TYPE_TINYINT:      {OID: pgtype.Int2OID, Size: 2},
	duckdb.TYPE_SMALLINT:     {OID: pgtype.Int2OID, Size: 2},
	duckdb.TYPE_INTEGER:      {OID: pgtype.Int4OID, Size: 4},
	duckdb.TYPE_BIGINT:       {OID: pgtype.Int8OID, Size: 8},
	duckdb.TYPE_UTINYINT:     {OID: pgtype.Int2OID, Size: 2},
	duckdb.TYPE_USMALLINT:    {OID: pgtype.Int4OID, Size: 4},
	duckdb.TYPE_UINTEGER:     {OID: pgtype.Int8OID, Size: 8},
	duckdb.TYPE_UBIGINT:      {OID: pgtype.NumericOID, Size: -1},
	duckdb.TYPE_FLOAT:        {OID: pgtype.Float4OID, Size: 4},
	duckdb.TYPE_DOUBLE:       {OID: pgtype.Float8OID, Size: 8},
	duckdb.TYPE_TIMESTAMP:    {OID: pgtype.TimestampOID, Size: 8},
	duckdb.TYPE_TIMESTAMP_S:  {OID: pgtype.TimestampOID, Size: 8},
	duckdb.TYPE_TIMESTAMP_MS: {OID: pgtype.TimestampOID, Size: 8},
	duckdb.TYPE_TIMESTAMP_NS: {OID: pgtype.TimestampOID, Size: 8},
	duckdb.TYPE_DATE:         {OID: pgtype.DateOID, Size: 4},
	duckdb.TYPE_TIME:         {OID: pgtype.TimeOID, Size: 8},
	duckdb.TYPE_TIME_TZ:      {OID: pgtype.TimetzOID, Size: 12},
	duckdb.TYPE_TIMESTAMP_TZ: {OID: pgtype.TimestamptzOID, Size: 8},
	duckdb.TYPE_INTERVAL:     {OID: pgtype.IntervalOID, Size: 16},
	duckdb.TYPE_HUGEINT:      {OID: pgtype.NumericOID, Size: -1},
	duckdb.TYPE_UHUGEINT:     {OID: pgtype.NumericOID, Size: -1},
	duckdb.TYPE_VARCHAR:      {OID: pgtype.TextOID, Size: -1},
	duckdb.TYPE_BLOB:         {OID: pgtype.ByteaOID, Size: -1},
	duckdb.TYPE_DECIMAL:      {OID: pgtype.NumericOID, Size: -1},
	duckdb.TYPE_ENUM:         {OID: pgtype.TextOID, Size: -1},
	duckdb.TYPE_LIST:         {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_STRUCT:       {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_MAP:          {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_ARRAY:        {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_UUID:         {OID: pgtype.UUIDOID, Size: 16},
	duckdb.TYPE_UNION:        {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_BIT:          {OID: pgtype.BitOID, Size: -1},
	duckdb.TYPE_BIGNUM:       {OID: pgtype.NumericOID, Size: -1},
	duckdb.TYPE_GEOMETRY:     {OID: pgtype.TextOID, Size: -1},
	duckdb.TYPE_VARIANT:      {OID: pgtype.JSONOID, Size: -1},
	duckdb.TYPE_SQLNULL:      {OID: pgtype.TextOID, Size: -1},
	duckdb.TYPE_ANY:          {OID: 0, Size: -1},
	duckdb.TYPE_INVALID:      {OID: 0, Size: -1},
}

func resolvePgTypeFromDuckdbType(t duckdb.Type) (oid uint32, size int16, typmod int32) {
	if meta, ok := duckdbTypeToPgMeta[t]; ok {
		return meta.OID, meta.Size, -1
	}
	return pgtype.TextOID, -1, -1
}

// resolvePgType maps a DuckDB column DatabaseTypeName to PostgreSQL OID, size, and typmod.
func resolvePgType(dbTypeName string) (oid uint32, size int16, typmod int32) {
	name := strings.TrimSpace(dbTypeName)
	upper := strings.ToUpper(name)

	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		prec, scale := parseDecimalParams(upper)
		typmod = int32(((prec << 16) | scale) + pgVarHdrSize)
		return pgtype.NumericOID, -1, typmod
	}

	base := upper
	if i := strings.IndexByte(upper, '('); i >= 0 {
		base = upper[:i]
	}
	if i := strings.IndexByte(base, '['); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(base)

	switch {
	case strings.HasPrefix(base, "LIST") || strings.HasPrefix(upper, "LIST") ||
		strings.HasPrefix(base, "ARRAY") || strings.Contains(upper, "[]"):
		return pgtype.JSONOID, -1, -1
	case strings.HasPrefix(base, "STRUCT") || strings.HasPrefix(upper, "STRUCT"):
		return pgtype.JSONOID, -1, -1
	case strings.HasPrefix(base, "MAP") || strings.HasPrefix(upper, "MAP"):
		return pgtype.JSONOID, -1, -1
	case strings.HasPrefix(base, "UNION") || strings.HasPrefix(upper, "UNION"):
		return pgtype.JSONOID, -1, -1
	}

	if meta, ok := duckdbTypeNameToPgMeta[base]; ok {
		return meta.OID, meta.Size, -1
	}
	return pgtype.TextOID, -1, -1
}

func parseDecimalParams(upper string) (prec, scale int) {
	prec, scale = 18, 3 // fallback when width/scale are absent (DuckDB DECIMAL default)
	start := strings.IndexByte(upper, '(')
	end := strings.IndexByte(upper, ')')
	if start < 0 || end <= start+1 {
		return prec, scale
	}
	parts := strings.Split(upper[start+1:end], ",")
	if len(parts) >= 1 {
		if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			prec = p
		}
	}
	if len(parts) >= 2 {
		if s, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			scale = s
		}
	}
	return prec, scale
}
