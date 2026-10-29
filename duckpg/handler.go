package duckpg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (pg *pgWire) dispatch(msg pgproto3.FrontendMessage) ([]byte, error) {
	if pg.session.ignoreUntilSync {
		switch msg.(type) {
		case *pgproto3.Sync:
			return pg.handleSync(msg.(*pgproto3.Sync))
		case *pgproto3.Terminate:
			return nil, nil
		default:
			// Discard messages until Sync after an extended-protocol error.
			return []byte{}, nil
		}
	}

	switch m := msg.(type) {
	case *pgproto3.Query:
		return pg.handleQuery(m)
	case *pgproto3.Parse:
		return pg.handleParse(m)
	case *pgproto3.Bind:
		return pg.handleBind(m)
	case *pgproto3.Describe:
		return pg.handleDescribe(m)
	case *pgproto3.Execute:
		return pg.handleExecute(m)
	case *pgproto3.Close:
		return pg.handleClose(m)
	case *pgproto3.Sync:
		return pg.handleSync(m)
	case *pgproto3.Flush:
		return pg.handleFlush(m)
	case *pgproto3.Terminate:
		return nil, nil
	default:
		buf, err := encodeErrorResponse(nil, fmt.Errorf("unsupported frontend message: %T", msg))
		if err != nil {
			return nil, err
		}
		return encodeReadyForQuery(buf, pg.session.txStatus)
	}
}

func (pg *pgWire) handleStartupMessage(_ *pgproto3.StartupMessage) ([]byte, error) {
	buf, err := encodeAuthenticationOk(nil)
	if err != nil {
		return nil, err
	}

	params := [][2]string{
		{"server_version", "15.0"},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"TimeZone", "UTC"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
	}
	for _, p := range params {
		buf, err = encodeParameterStatus(buf, p[0], p[1])
		if err != nil {
			return nil, err
		}
	}

	buf, err = encodeBackendKeyData(buf, pg.session.processID, pg.session.secretKey)
	if err != nil {
		return nil, err
	}

	return encodeReadyForQuery(buf, pg.session.txStatus)
}

func (pg *pgWire) handleSSLRequest() ([]byte, error) {
	return []byte{'N'}, nil
}

func (pg *pgWire) handleQuery(msg *pgproto3.Query) ([]byte, error) {
	query := msg.String
	infoLog.Printf("received sql query: %s\n", query)

	// Simple Query closes the unnamed statement and portal.
	pg.session.clearUnnamed()

	trimmed := strings.TrimSpace(query)
	if trimmed == "" || trimmed == ";" {
		buf, err := encodeEmptyQueryResponse(nil)
		if err != nil {
			return nil, err
		}
		return encodeReadyForQuery(buf, pg.session.txStatus)
	}

	return pg.executeSimpleQuery(query)
}

func (pg *pgWire) executeSimpleQuery(query string) ([]byte, error) {
	ctx := context.Background()

	if prefersExec(query) {
		return pg.executeExec(ctx, query)
	}
	return pg.executeQuery(ctx, query)
}

func prefersExec(query string) bool {
	switch firstSQLKeyword(query) {
	case "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE",
		"BEGIN", "START", "COMMIT", "ROLLBACK", "ABORT",
		"SET", "RESET", "ATTACH", "DETACH", "LOAD", "INSTALL",
		"COPY", "ANALYZE", "VACUUM", "CHECKPOINT", "USE", "GRANT", "REVOKE":
		return true
	default:
		return false
	}
}

func (pg *pgWire) executeExec(ctx context.Context, query string) ([]byte, error) {
	result, err := pg.session.conn.ExecContext(ctx, query)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, err)
		buf, encErr := encodeErrorResponse(nil, err)
		if encErr != nil {
			return nil, encErr
		}
		return encodeReadyForQuery(buf, pg.session.txStatus)
	}

	rowsAffected := int64(0)
	if n, raErr := result.RowsAffected(); raErr == nil {
		rowsAffected = n
	}

	pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, nil)
	tag := commandTag(query, false, 0, rowsAffected)

	buf, err := encodeCommandComplete(nil, tag)
	if err != nil {
		return nil, err
	}
	return encodeReadyForQuery(buf, pg.session.txStatus)
}

func (pg *pgWire) executeQuery(ctx context.Context, query string) ([]byte, error) {
	rows, err := pg.session.conn.QueryContext(ctx, query)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, err)
		buf, encErr := encodeErrorResponse(nil, err)
		if encErr != nil {
			return nil, encErr
		}
		return encodeReadyForQuery(buf, pg.session.txStatus)
	}
	defer rows.Close()

	buf, oids, err := encodeRowDescription(nil, rows)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, err)
		ebuf, encErr := encodeErrorResponse(nil, err)
		if encErr != nil {
			return nil, encErr
		}
		return encodeReadyForQuery(ebuf, pg.session.txStatus)
	}

	buf, rowCnt, err := encodeDataRow(buf, rows, oids)
	if err != nil {
		pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, err)
		ebuf, encErr := encodeErrorResponse(nil, err)
		if encErr != nil {
			return nil, encErr
		}
		return encodeReadyForQuery(ebuf, pg.session.txStatus)
	}

	pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, nil)
	tag := commandTag(query, true, int64(rowCnt), -1)

	buf, err = encodeCommandComplete(buf, tag)
	if err != nil {
		return nil, err
	}
	return encodeReadyForQuery(buf, pg.session.txStatus)
}
