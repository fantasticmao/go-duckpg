package duckpg

import (
	"context"
	"database/sql"
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

func (pg *pgWire) handleQuery(msg *pgproto3.Query) ([]byte, error) {
	query := msg.String
	logSQL(pg, query)

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

	return pg.runSimpleQuery(query)
}

// simpleError reports a simple-query failure: transaction status, log, ErrorResponse, and ReadyForQuery.
func (pg *pgWire) simpleError(query string, err error) ([]byte, error) {
	pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, err)
	logSQLFailed(pg, query, err)
	buf, encErr := encodeErrorResponse(nil, err)
	if encErr != nil {
		return nil, encErr
	}
	return encodeReadyForQuery(buf, pg.session.txStatus)
}

func (pg *pgWire) runSimpleQuery(query string) ([]byte, error) {
	ctx, stop := pg.session.queryContext()
	defer stop()
	class := classifyCompat(query, pg.session)
	switch class.kind {
	case compatLocal:
		pg.session.applyEffect(class.effect)
		buf, err := pg.completeCommand(nil, query, class.tag)
		if err != nil {
			return nil, err
		}
		return encodeReadyForQuery(buf, pg.session.txStatus)
	case compatRewrite:
		pg.session.applyEffect(class.effect)
		query = class.sql
	}

	if prefersExec(query) {
		return pg.execSimple(ctx, query)
	}
	return pg.querySimple(ctx, query)
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

// completeCommand records a successful statement and appends CommandComplete.
// Simple queries append ReadyForQuery themselves. The extended protocol waits for Sync.
func (pg *pgWire) completeCommand(buf []byte, query, tag string) ([]byte, error) {
	pg.session.txStatus = updateTxStatus(pg.session.txStatus, query, nil)
	return encodeCommandComplete(buf, tag)
}

func rowsAffected(result sql.Result) int64 {
	n, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

func (pg *pgWire) execSimple(ctx context.Context, query string) ([]byte, error) {
	result, err := pg.session.conn.ExecContext(ctx, query)
	if err != nil {
		return pg.simpleError(query, err)
	}

	buf, err := pg.completeCommand(nil, query, commandTag(query, false, 0, rowsAffected(result)))
	if err != nil {
		return nil, err
	}
	return encodeReadyForQuery(buf, pg.session.txStatus)
}

func (pg *pgWire) querySimple(ctx context.Context, query string) ([]byte, error) {
	rows, err := pg.session.conn.QueryContext(ctx, query)
	if err != nil {
		return pg.simpleError(query, err)
	}
	defer rows.Close()

	buf, oids, err := encodeRowDescription(nil, rows)
	if err != nil {
		return pg.simpleError(query, err)
	}

	buf, rowCnt, err := encodeDataRow(buf, rows, oids)
	if err != nil {
		return pg.simpleError(query, err)
	}

	buf, err = pg.completeCommand(buf, query, commandTag(query, true, int64(rowCnt), -1))
	if err != nil {
		return nil, err
	}
	return encodeReadyForQuery(buf, pg.session.txStatus)
}
