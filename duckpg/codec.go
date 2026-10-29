package duckpg

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	rowDescTableOID             = uint32(0)
	rowDescTableAttributeNumber = uint16(0)
	rowDescFormatText           = int16(0)
)

type rowData struct {
	data any
	oid  uint32
}

func (dst *rowData) Scan(src any) error {
	dst.data = src
	return nil
}

func (dst *rowData) toBytes() ([]byte, error) {
	if dst.data == nil {
		return nil, nil
	}
	return encodeTextValue(dst.data, dst.oid)
}

func encodeTextValue(data any, oid uint32) ([]byte, error) {
	switch d := data.(type) {
	case nil:
		return nil, nil
	case bool:
		if d {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int8:
		return []byte(strconv.FormatInt(int64(d), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(d), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(d), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(d, 10)), nil
	case int:
		return []byte(strconv.FormatInt(int64(d), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(d), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(d), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(d), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(d, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(d), 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(d), 'f', -1, 32)), nil
	case float64:
		return []byte(strconv.FormatFloat(d, 'f', -1, 64)), nil
	case string:
		return []byte(d), nil
	case []byte:
		return encodeByteSlice(d, oid)
	case time.Time:
		return []byte(formatTime(d, oid)), nil
	case *big.Int:
		if d == nil {
			return nil, nil
		}
		return []byte(d.String()), nil
	case big.Int:
		return []byte(d.String()), nil
	case duckdb.Decimal:
		return []byte(d.String()), nil
	case duckdb.Interval:
		return []byte(formatInterval(d)), nil
	case duckdb.Union:
		b, err := json.Marshal(d)
		return b, err
	case *duckdb.OrderedMap:
		return d.MarshalJSON()
	case duckdb.OrderedMap:
		return d.MarshalJSON()
	case []any:
		return json.Marshal(d)
	case map[string]any:
		return json.Marshal(d)
	case json.RawMessage:
		return d, nil
	default:
		// Fallback: try JSON, then fmt
		if b, err := json.Marshal(d); err == nil {
			return b, nil
		}
		return []byte(fmt.Sprint(d)), nil
	}
}

func encodeByteSlice(d []byte, oid uint32) ([]byte, error) {
	switch oid {
	case pgtype.UUIDOID:
		if len(d) == 16 {
			return []byte(formatUUID(d)), nil
		}
		return d, nil
	case pgtype.ByteaOID:
		dst := make([]byte, 2+hex.EncodedLen(len(d)))
		dst[0], dst[1] = '\\', 'x'
		hex.Encode(dst[2:], d)
		return dst, nil
	default:
		return d, nil
	}
}

func formatUUID(b []byte) string {
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
		b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])
}

func formatTime(t time.Time, oid uint32) string {
	switch oid {
	case pgtype.DateOID:
		return t.Format("2006-01-02")
	case pgtype.TimeOID:
		return t.Format("15:04:05.999999")
	case pgtype.TimetzOID:
		return t.Format("15:04:05.999999-07")
	case pgtype.TimestamptzOID:
		return t.UTC().Format("2006-01-02 15:04:05.999999+00")
	case pgtype.TimestampOID:
		return t.Format("2006-01-02 15:04:05.999999")
	default:
		return t.Format("2006-01-02 15:04:05.999999")
	}
}

func formatInterval(iv duckdb.Interval) string {
	parts := make([]string, 0, 3)
	if iv.Months != 0 {
		years := iv.Months / 12
		months := iv.Months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}
		if months != 0 {
			parts = append(parts, fmt.Sprintf("%d mons", months))
		}
	}
	if iv.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", iv.Days))
	}
	if iv.Micros != 0 || len(parts) == 0 {
		neg := ""
		us := iv.Micros
		if us < 0 {
			neg = "-"
			us = -us
		}
		hours := us / 3_600_000_000
		us %= 3_600_000_000
		mins := us / 60_000_000
		us %= 60_000_000
		secs := us / 1_000_000
		us %= 1_000_000
		if us != 0 {
			parts = append(parts, fmt.Sprintf("%s%d:%02d:%02d.%06d", neg, hours, mins, secs, us))
		} else {
			parts = append(parts, fmt.Sprintf("%s%d:%02d:%02d", neg, hours, mins, secs))
		}
	}
	return strings.Join(parts, " ")
}

func encodeRowDescription(buf []byte, rows *sql.Rows) ([]byte, []uint32, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	oids := make([]uint32, len(colTypes))
	fields := make([]pgproto3.FieldDescription, len(colTypes))
	for i, colType := range colTypes {
		oid, size, typmod := resolvePgType(colType.DatabaseTypeName())
		oids[i] = oid
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(colType.Name()),
			TableOID:             rowDescTableOID,
			TableAttributeNumber: rowDescTableAttributeNumber,
			DataTypeOID:          oid,
			DataTypeSize:         size,
			TypeModifier:         typmod,
			Format:               rowDescFormatText,
		}
	}
	buf, err = (&pgproto3.RowDescription{Fields: fields}).Encode(buf)
	return buf, oids, err
}

func encodeDataRow(buf []byte, rows *sql.Rows, oids []uint32) ([]byte, int, error) {
	colLen := len(oids)
	rowCnt := 0
	for ; rows.Next(); rowCnt++ {
		row := make([]rowData, colLen)
		dataScan := make([]any, colLen)
		for i := range row {
			row[i].oid = oids[i]
			dataScan[i] = &row[i]
		}

		if err := rows.Scan(dataScan...); err != nil {
			return buf, rowCnt, err
		}

		values := make([][]byte, colLen)
		for i := range row {
			var err error
			values[i], err = row[i].toBytes()
			if err != nil {
				return buf, rowCnt, err
			}
		}
		var err error
		buf, err = (&pgproto3.DataRow{Values: values}).Encode(buf)
		if err != nil {
			return buf, rowCnt, err
		}
	}
	if err := rows.Err(); err != nil {
		return buf, rowCnt, err
	}
	return buf, rowCnt, nil
}

func encodeCommandComplete(buf []byte, tag string) ([]byte, error) {
	return (&pgproto3.CommandComplete{CommandTag: []byte(tag)}).Encode(buf)
}

func encodeReadyForQuery(buf []byte, txStatus byte) ([]byte, error) {
	return (&pgproto3.ReadyForQuery{TxStatus: txStatus}).Encode(buf)
}

func encodeErrorResponse(buf []byte, err error) ([]byte, error) {
	msg := err.Error()
	return (&pgproto3.ErrorResponse{
		Severity:            "ERROR",
		SeverityUnlocalized: "ERROR",
		Code:                "XX000",
		Message:             msg,
	}).Encode(buf)
}

func encodeEmptyQueryResponse(buf []byte) ([]byte, error) {
	return (&pgproto3.EmptyQueryResponse{}).Encode(buf)
}

func encodeAuthenticationOk(buf []byte) ([]byte, error) {
	return (&pgproto3.AuthenticationOk{}).Encode(buf)
}

func encodeBackendKeyData(buf []byte, processID uint32, secretKey []byte) ([]byte, error) {
	return (&pgproto3.BackendKeyData{
		ProcessID: processID,
		SecretKey: secretKey,
	}).Encode(buf)
}

func encodeParameterStatus(buf []byte, name, value string) ([]byte, error) {
	return (&pgproto3.ParameterStatus{Name: name, Value: value}).Encode(buf)
}
