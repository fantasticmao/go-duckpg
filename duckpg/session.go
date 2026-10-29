package duckpg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"
)

var nextBackendPID atomic.Uint32

func init() {
	nextBackendPID.Store(1000)
}

// preparedStmt is a parsed statement retained for the extended query protocol.
type preparedStmt struct {
	name      string
	query     string
	paramOIDs []uint32
	fields    []pgproto3.FieldDescription // nil means no result columns (NoData)
	localTag  string                      // non-empty: Execute returns this tag and skips DuckDB
	effect    compatEffect
}

// portal is a bound statement ready for Execute.
type portal struct {
	name          string
	stmt          *preparedStmt
	args          []any
	resultFormats []int16
	described     bool // RowDescription already sent for this portal
}

// session holds per-client DuckDB connection state.
type session struct {
	conn            *sql.Conn
	txStatus        byte // 'I' idle, 'T' in transaction, 'E' failed transaction
	processID       uint32
	secretKey       []byte // 4-byte classic cancel key
	statements      map[string]*preparedStmt
	portals         map[string]*portal
	ignoreUntilSync bool
	settings        map[string]string // lowercase PostgreSQL GUC name → session value
}

func newSession(db *sql.DB) (*session, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	s := &session{
		conn:       conn,
		txStatus:   'I',
		processID:  nextBackendPID.Add(1),
		secretKey:  randomSecretKey(),
		statements: make(map[string]*preparedStmt),
		portals:    make(map[string]*portal),
		settings:   make(map[string]string),
	}
	return s, nil
}

func (s *session) close() error {
	s.statements = nil
	s.portals = nil
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}

// clearUnnamed drops the unnamed prepared statement and portal (simple Query rule).
func (s *session) clearUnnamed() {
	delete(s.statements, "")
	delete(s.portals, "")
}

func randomSecretKey() []byte {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		binary.BigEndian.PutUint32(b, nextBackendPID.Load()^0xdeadbeef)
	}
	return b
}

// commandTag builds a PostgreSQL CommandComplete tag from the SQL and result shape.
func commandTag(query string, hasResultSet bool, rowCnt int64, rowsAffected int64) string {
	kw := firstSQLKeyword(query)
	switch kw {
	case "SELECT":
		return "SELECT " + itoa64(rowCnt)
	case "INSERT":
		n := rowsAffected
		if hasResultSet {
			n = rowCnt
		}
		return "INSERT 0 " + itoa64(n)
	case "UPDATE":
		n := rowsAffected
		if hasResultSet {
			n = rowCnt
		}
		return "UPDATE " + itoa64(n)
	case "DELETE":
		n := rowsAffected
		if hasResultSet {
			n = rowCnt
		}
		return "DELETE " + itoa64(n)
	case "BEGIN", "START":
		return "BEGIN"
	case "COMMIT":
		return "COMMIT"
	case "ROLLBACK", "ABORT":
		return "ROLLBACK"
	case "CREATE", "DROP", "ALTER", "TRUNCATE", "COPY", "ATTACH", "DETACH",
		"CALL", "PRAGMA", "SET", "RESET", "LOAD", "INSTALL", "EXPORT", "IMPORT",
		"ANALYZE", "VACUUM", "COMMENT", "GRANT", "REVOKE", "PREPARE", "EXECUTE",
		"DEALLOCATE", "EXPLAIN", "DESCRIBE", "SHOW", "USE", "CHECKPOINT":
		return kw
	}
	if hasResultSet {
		return "SELECT " + itoa64(rowCnt)
	}
	if rowsAffected >= 0 {
		return "SELECT " + itoa64(rowsAffected)
	}
	return "SELECT 0"
}

func firstSQLKeyword(query string) string {
	s := strings.TrimSpace(query)
	if s == "" {
		return ""
	}
	// Strip leading -- and /* */ comments.
	for {
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = strings.TrimSpace(s[i+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = strings.TrimSpace(s[i+2:])
				continue
			}
			return ""
		}
		break
	}
	end := 0
	for end < len(s) {
		c := s[end]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			end++
			continue
		}
		break
	}
	return strings.ToUpper(s[:end])
}

func updateTxStatus(status byte, query string, queryErr error) byte {
	kw := firstSQLKeyword(query)
	switch kw {
	case "BEGIN", "START":
		if queryErr == nil {
			return 'T'
		}
		return status
	case "COMMIT":
		if queryErr == nil {
			return 'I'
		}
		return 'E'
	case "ROLLBACK", "ABORT":
		return 'I'
	}
	if queryErr != nil && status == 'T' {
		return 'E'
	}
	return status
}

func itoa64(n int64) string {
	if n < 0 {
		n = 0
	}
	return strconv.FormatInt(n, 10)
}
