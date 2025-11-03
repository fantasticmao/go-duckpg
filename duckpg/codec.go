package duckpg

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"strconv"
	"time"
)

const (
	rowDescTableOID             = uint32(0)
	rowDescTableAttributeNumber = uint16(0)
	rowDescTypeModifier         = int32(-1)
	rowDescFormat               = int16(0)
)

func duckdbIntToBytes(data any) ([]byte, error) {
	return nil, fmt.Errorf("unsupported duckdb data type: %#v", data)
}

type rowData struct {
	data any
}

func (dst *rowData) Scan(src any) error {
	dst.data = src
	return nil
}

func (dst *rowData) toBytes() ([]byte, error) {
	switch d := dst.data.(type) {
	case nil:
		return nil, nil
	case []byte:
		return d, nil
	case string:
		return []byte(d), nil
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.BigEndian, 1)
		err = binary.Write(buf, binary.BigEndian, d)
		return buf.Bytes(), err
	case time.Time:
		return []byte(d.Format(time.RFC3339)), nil
	case []any, map[any]any, map[string]any:
		return json.Marshal(d)
	default:
		return nil, fmt.Errorf("unsupported duckdb data type: %#v", d)
	}
}

// Message format of RowDescription: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
func encodeRowDescription(buf []byte, rows *sql.Rows) ([]byte, int, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, -1, err
	}

	colLen := len(colTypes)
	rowDesc := make([]pgproto3.FieldDescription, colLen)
	for i, colType := range colTypes {
		// FIXME determine the correct DataTypeOID and DataTypeSize
		colName := colType.Name()
		var oid uint32 = 25
		if duckdbType, ok1 := goTypeToDuckdbTypeMap[colType.ScanType()]; ok1 {
			if pgOid, ok2 := duckdbTypeToPgOidMap[duckdbType]; ok2 {
				oid = pgOid
			}
		}

		rowDesc[i] = pgproto3.FieldDescription{
			Name:                 []byte(colName),
			TableOID:             rowDescTableOID,
			TableAttributeNumber: rowDescTableAttributeNumber,
			DataTypeOID:          oid,
			// FIXME
			DataTypeSize: int16(-1),
			TypeModifier: rowDescTypeModifier,
			Format:       rowDescFormat,
		}
	}
	buf, err = (&pgproto3.RowDescription{Fields: rowDesc}).Encode(buf)
	return buf, colLen, err
}

// Message format of DataRow: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DATAROW
func encodeDataRow(buf []byte, rows *sql.Rows, colLen int) ([]byte, int, error) {
	rowCnt := 0
	for ; rows.Next(); rowCnt++ {
		row := make([]rowData, colLen)
		for i := range row {
			row[i] = rowData{}
		}

		dataScan := make([]any, colLen)
		for i := range row {
			dataScan[i] = &row[i]
		}

		err := rows.Scan(dataScan...)
		if err != nil {
			return buf, rowCnt, err
		}

		values := make([][]byte, colLen)
		for i := range row {
			values[i], err = row[i].toBytes()
			if err != nil {
				return buf, rowCnt, err
			}
		}
		buf, err = (&pgproto3.DataRow{Values: values}).Encode(buf)
		if err != nil {
			return buf, rowCnt, err
		}
	}
	return buf, rowCnt, nil
}

// Message format of CommandComplete: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
func encodeCommandComplete(buf []byte, rowCnt int) ([]byte, error) {
	buf, err := (&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(rowCnt))}).Encode(buf)
	if err != nil {
		return buf, err
	}
	return buf, nil
}

// Message format of ReadyForQuery: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY
func encodeReadyForQuery(buf []byte) ([]byte, error) {
	buf, err := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		return buf, err
	}
	return buf, nil
}
