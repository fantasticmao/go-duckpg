package duckpg

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

var pgTypeMap = pgtype.NewMap()

func (pg *pgWire) extendedError(err error) ([]byte, error) {
	pg.session.ignoreUntilSync = true
	if pg.session.txStatus == 'T' {
		pg.session.txStatus = 'E'
	}
	return encodeErrorResponse(nil, err)
}

func (pg *pgWire) handleParse(msg *pgproto3.Parse) ([]byte, error) {
	ps, err := pg.inspectPrepare(msg.Query, msg.ParameterOIDs)
	if err != nil {
		return pg.extendedError(err)
	}
	ps.name = msg.Name
	pg.session.statements[msg.Name] = ps
	for pname, p := range pg.session.portals {
		if p.stmt != nil && p.stmt.name == msg.Name {
			delete(pg.session.portals, pname)
		}
	}
	return encodeParseComplete(nil)
}

func (pg *pgWire) inspectPrepare(query string, clientParamOIDs []uint32) (*preparedStmt, error) {
	class := classifyCompat(query, pg.session)
	if class.kind == compatLocal {
		return &preparedStmt{
			query:    query,
			localTag: class.tag,
			effect:   class.effect,
		}, nil
	}
	if class.kind == compatRewrite {
		query = class.sql
	}

	var ps *preparedStmt
	ctx, stop := pg.session.queryContext()
	defer stop()
	err := pg.session.conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type %T", driverConn)
		}
		stmt, err := dc.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		ds := stmt.(*duckdb.Stmt)
		defer ds.Close()

		nParams := ds.NumInput()
		paramOIDs := make([]uint32, nParams)
		for i := 0; i < nParams; i++ {
			if i < len(clientParamOIDs) && clientParamOIDs[i] != 0 {
				paramOIDs[i] = clientParamOIDs[i]
				continue
			}
			dt, err := ds.ParamType(i + 1) // 1-based
			if err != nil {
				return err
			}
			oid, _, _ := resolvePgTypeFromDuckdbType(dt)
			paramOIDs[i] = oid
		}

		colCount, err := ds.ColumnCount()
		if err != nil {
			return err
		}
		var fields []pgproto3.FieldDescription
		if colCount > 0 {
			fields = make([]pgproto3.FieldDescription, colCount)
			for i := 0; i < colCount; i++ {
				fields[i], err = fieldDescriptionFromStmt(ds, i)
				if err != nil {
					return err
				}
			}
		}

		ps = &preparedStmt{
			query:     query,
			paramOIDs: paramOIDs,
			fields:    fields,
			effect:    class.effect,
		}
		return nil
	})
	return ps, err
}

func fieldDescriptionFromStmt(ds *duckdb.Stmt, i int) (pgproto3.FieldDescription, error) {
	name, err := ds.ColumnName(i)
	if err != nil {
		return pgproto3.FieldDescription{}, err
	}
	dt, err := ds.ColumnType(i)
	if err != nil {
		return pgproto3.FieldDescription{}, err
	}
	oid, size, typmod := resolvePgTypeFromDuckdbType(dt)
	if dt == duckdb.TYPE_DECIMAL {
		if info, infoErr := ds.ColumnTypeInfo(i); infoErr == nil && info != nil {
			if details, ok := info.Details().(*duckdb.DecimalDetails); ok {
				typmod = int32(((int(details.Width) << 16) | int(details.Scale)) + pgVarHdrSize)
			}
		}
	}
	return pgproto3.FieldDescription{
		Name:                 []byte(name),
		TableOID:             rowDescTableOID,
		TableAttributeNumber: rowDescTableAttributeNumber,
		DataTypeOID:          oid,
		DataTypeSize:         size,
		TypeModifier:         typmod,
		Format:               rowDescFormatText,
	}, nil
}

func (pg *pgWire) handleBind(msg *pgproto3.Bind) ([]byte, error) {
	ps, ok := pg.session.statements[msg.PreparedStatement]
	if !ok {
		return pg.extendedError(fmt.Errorf(`prepared statement "%s" does not exist`, msg.PreparedStatement))
	}
	if len(msg.Parameters) != len(ps.paramOIDs) {
		return pg.extendedError(fmt.Errorf(
			"bind message supplies %d parameters, but prepared statement requires %d",
			len(msg.Parameters), len(ps.paramOIDs),
		))
	}

	args := make([]any, len(msg.Parameters))
	for i, param := range msg.Parameters {
		format := formatCodeAt(msg.ParameterFormatCodes, i)
		oid := uint32(0)
		if i < len(ps.paramOIDs) {
			oid = ps.paramOIDs[i]
		}
		v, err := decodeParam(oid, format, param)
		if err != nil {
			return pg.extendedError(fmt.Errorf("bind parameter $%d: %w", i+1, err))
		}
		args[i] = v
	}

	pg.session.portals[msg.DestinationPortal] = &portal{
		name:          msg.DestinationPortal,
		stmt:          ps,
		args:          args,
		resultFormats: append([]int16(nil), msg.ResultFormatCodes...),
		described:     false,
	}
	return encodeBindComplete(nil)
}

func formatCodeAt(codes []int16, i int) int16 {
	switch len(codes) {
	case 0:
		return pgtype.TextFormatCode
	case 1:
		return codes[0]
	default:
		if i < len(codes) {
			return codes[i]
		}
		return pgtype.TextFormatCode
	}
}

func applyResultFormats(fields []pgproto3.FieldDescription, formats []int16) []pgproto3.FieldDescription {
	out := make([]pgproto3.FieldDescription, len(fields))
	copy(out, fields)
	for i := range out {
		out[i].Format = formatCodeAt(formats, i)
	}
	return out
}

func decodeParam(oid uint32, format int16, data []byte) (any, error) {
	if data == nil {
		return nil, nil
	}
	if oid == 0 {
		if format == pgtype.BinaryFormatCode {
			return nil, fmt.Errorf("binary format with unspecified type OID")
		}
		return string(data), nil
	}

	switch oid {
	case pgtype.BoolOID:
		var v bool
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.Int2OID:
		var v int16
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.Int4OID:
		var v int32
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.Int8OID:
		var v int64
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.Float4OID:
		var v float32
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.Float8OID:
		var v float64
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.NameOID, pgtype.JSONOID, pgtype.JSONBOID, pgtype.BitOID:
		var v string
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.ByteaOID:
		var v []byte
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	case pgtype.NumericOID:
		var v pgtype.Numeric
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		if !v.Valid {
			return nil, nil
		}
		text, err := pgTypeMap.Encode(pgtype.NumericOID, pgtype.TextFormatCode, v, nil)
		if err != nil {
			return nil, err
		}
		return string(text), nil
	case pgtype.UUIDOID:
		var v [16]byte
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v[:], nil
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TimeOID, pgtype.TimetzOID:
		var v time.Time
		if err := pgTypeMap.Scan(oid, format, data, &v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		if format == pgtype.TextFormatCode {
			return string(data), nil
		}
		return nil, fmt.Errorf("unsupported parameter type OID %d in binary format", oid)
	}
}

func (pg *pgWire) handleDescribe(msg *pgproto3.Describe) ([]byte, error) {
	switch msg.ObjectType {
	case 'S':
		ps, ok := pg.session.statements[msg.Name]
		if !ok {
			return pg.extendedError(fmt.Errorf(`prepared statement "%s" does not exist`, msg.Name))
		}
		buf, err := encodeParameterDescription(nil, ps.paramOIDs)
		if err != nil {
			return nil, err
		}
		if len(ps.fields) == 0 {
			return encodeNoData(buf)
		}
		// Statement descriptions always advertise text format; Bind chooses the result format.
		return encodeRowDescriptionFields(buf, ps.fields)
	case 'P':
		p, ok := pg.session.portals[msg.Name]
		if !ok {
			return pg.extendedError(fmt.Errorf(`portal "%s" does not exist`, msg.Name))
		}
		if len(p.stmt.fields) == 0 {
			p.described = true
			return encodeNoData(nil)
		}
		p.described = true
		return encodeRowDescriptionFields(nil, applyResultFormats(p.stmt.fields, p.resultFormats))
	default:
		return pg.extendedError(fmt.Errorf("unsupported Describe object type %q", msg.ObjectType))
	}
}

func (pg *pgWire) handleExecute(msg *pgproto3.Execute) ([]byte, error) {
	p, ok := pg.session.portals[msg.Portal]
	if !ok {
		return pg.extendedError(fmt.Errorf(`portal "%s" does not exist`, msg.Portal))
	}
	ctx, stop := pg.session.queryContext()
	defer stop()
	if len(p.stmt.fields) == 0 {
		return pg.executePortalExec(ctx, p)
	}
	return pg.executePortalQuery(ctx, p)
}

func (pg *pgWire) executePortalExec(ctx context.Context, p *portal) ([]byte, error) {
	ps := p.stmt
	if ps.localTag != "" {
		pg.session.applyEffect(ps.effect)
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, nil)
		return encodeCommandComplete(nil, ps.localTag)
	}
	result, err := pg.session.conn.ExecContext(ctx, ps.query, p.args...)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, err)
		return pg.extendedError(err)
	}
	rowsAffected := int64(0)
	if n, raErr := result.RowsAffected(); raErr == nil {
		rowsAffected = n
	}
	pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, nil)
	tag := commandTag(ps.query, false, 0, rowsAffected)
	return encodeCommandComplete(nil, tag)
}

func (pg *pgWire) executePortalQuery(ctx context.Context, p *portal) ([]byte, error) {
	ps := p.stmt
	pg.session.applyEffect(ps.effect)
	rows, err := pg.session.conn.QueryContext(ctx, ps.query, p.args...)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, err)
		return pg.extendedError(err)
	}
	defer rows.Close()

	var buf []byte
	if !p.described {
		// Portal was not described. Send RowDescription using the Bind result formats
		// so clients decode DataRow with the same format codes.
		buf, err = encodeRowDescriptionFields(nil, applyResultFormats(ps.fields, p.resultFormats))
		if err != nil {
			return nil, err
		}
		p.described = true
	}

	oids := make([]uint32, len(ps.fields))
	for i, f := range ps.fields {
		oids[i] = f.DataTypeOID
	}

	buf, rowCnt, err := encodeDataRowFormatted(buf, rows, oids, p.resultFormats)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, err)
		return pg.extendedError(err)
	}

	pg.session.txStatus = updateTxStatus(pg.session.txStatus, ps.query, nil)
	tag := commandTag(ps.query, true, int64(rowCnt), -1)
	return encodeCommandComplete(buf, tag)
}

func (pg *pgWire) handleClose(msg *pgproto3.Close) ([]byte, error) {
	switch msg.ObjectType {
	case 'S':
		delete(pg.session.statements, msg.Name)
		for pname, p := range pg.session.portals {
			if p.stmt != nil && p.stmt.name == msg.Name {
				delete(pg.session.portals, pname)
			}
		}
	case 'P':
		delete(pg.session.portals, msg.Name)
	default:
		return pg.extendedError(fmt.Errorf("unsupported Close object type %q", msg.ObjectType))
	}
	return encodeCloseComplete(nil)
}

func (pg *pgWire) handleSync(_ *pgproto3.Sync) ([]byte, error) {
	pg.session.ignoreUntilSync = false
	return encodeReadyForQuery(nil, pg.session.txStatus)
}

func (pg *pgWire) handleFlush(_ *pgproto3.Flush) ([]byte, error) {
	// Responses are written immediately after each message; nothing to flush.
	return []byte{}, nil
}

func encodeDataRowFormatted(buf []byte, rows *sql.Rows, oids []uint32, resultFormats []int16) ([]byte, int, error) {
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
			format := formatCodeAt(resultFormats, i)
			var err error
			values[i], err = encodeFormattedValue(row[i].data, oids[i], format)
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

func encodeFormattedValue(data any, oid uint32, format int16) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	data = normalizeDriverValue(data)
	if format == pgtype.TextFormatCode {
		return encodeTextValue(data, oid)
	}
	val, err := valueForBinaryEncode(data, oid)
	if err != nil {
		return nil, err
	}
	encoded, err := pgTypeMap.Encode(oid, pgtype.BinaryFormatCode, val, nil)
	if err != nil {
		return nil, fmt.Errorf("binary encode OID %d: %w", oid, err)
	}
	return encoded, nil
}

func valueForBinaryEncode(data any, oid uint32) (any, error) {
	switch oid {
	case pgtype.NumericOID:
		text, err := encodeTextValue(data, oid)
		if err != nil {
			return nil, err
		}
		var n pgtype.Numeric
		if err := pgTypeMap.Scan(pgtype.NumericOID, pgtype.TextFormatCode, text, &n); err != nil {
			return nil, err
		}
		return n, nil
	case pgtype.IntervalOID:
		text, err := encodeTextValue(data, oid)
		if err != nil {
			return nil, err
		}
		var iv pgtype.Interval
		if err := pgTypeMap.Scan(pgtype.IntervalOID, pgtype.TextFormatCode, text, &iv); err != nil {
			return nil, err
		}
		return iv, nil
	case pgtype.UUIDOID:
		switch d := data.(type) {
		case []byte:
			if len(d) == 16 {
				var u [16]byte
				copy(u[:], d)
				return u, nil
			}
		case string:
			var u [16]byte
			if err := pgTypeMap.Scan(pgtype.UUIDOID, pgtype.TextFormatCode, []byte(d), &u); err != nil {
				return nil, err
			}
			return u, nil
		}
		return data, nil
	case pgtype.ByteaOID:
		switch d := data.(type) {
		case []byte:
			return d, nil
		case string:
			return []byte(d), nil
		}
		return data, nil
	default:
		return data, nil
	}
}

func normalizeDriverValue(data any) any {
	switch d := data.(type) {
	case duckdb.Decimal:
		return d.String()
	case duckdb.Interval:
		return formatInterval(d)
	case *big.Int:
		if d == nil {
			return nil
		}
		return d.String()
	case big.Int:
		return d.String()
	case duckdb.Union:
		b, err := encodeTextValue(d, pgtype.JSONOID)
		if err != nil {
			return fmt.Sprint(d)
		}
		return string(b)
	case *duckdb.OrderedMap:
		b, err := d.MarshalJSON()
		if err != nil {
			return d.String()
		}
		return string(b)
	case duckdb.OrderedMap:
		b, err := d.MarshalJSON()
		if err != nil {
			return d.String()
		}
		return string(b)
	case []any, map[string]any:
		b, err := encodeTextValue(d, pgtype.JSONOID)
		if err != nil {
			return fmt.Sprint(d)
		}
		return string(b)
	default:
		return data
	}
}
