package duckpg

import (
	"database/sql"
	"github.com/jackc/pgx/v5/pgproto3"
	"reflect"
	"sync"
)

// handlerMapping maps message types to their respective handlers.
var handlerMapping = map[reflect.Type]func(msg pgproto3.FrontendMessage) ([]byte, error){
	reflect.TypeOf(&pgproto3.StartupMessage{}): handleStartupMessage,
	reflect.TypeOf(&pgproto3.SSLRequest{}):     handleSSLRequest,
	reflect.TypeOf(&pgproto3.Query{}):          handleQuery,
	reflect.TypeOf(&pgproto3.Terminate{}):      handleTerminate,
}

// database is the global database connection used by the message handlers.
var database *sql.DB

// databaseInitOnce ensures that the database connection is initialized only once.
var databaseInitOnce = sync.Once{}

func initDatabase(db *sql.DB) {
	databaseInitOnce.Do(func() {
		database = db
	})
}

func handleStartupMessage(msg pgproto3.FrontendMessage) ([]byte, error) {
	buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
	if err != nil {
		return nil, err
	}

	buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func handleSSLRequest(msg pgproto3.FrontendMessage) ([]byte, error) {
	buf := []byte("N")
	return buf, nil
}

func handleQuery(msg pgproto3.FrontendMessage) ([]byte, error) {
	query := msg.(*pgproto3.Query).String
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	buf, colLen, err := encodeRowDescription(nil, rows)
	if err != nil {
		return buf, err
	}

	buf, rowCnt, err := encodeDataRow(buf, rows, colLen)
	if err != nil {
		return buf, err
	}

	buf, err = encodeCommandComplete(buf, rowCnt)
	if err != nil {
		return buf, err
	}

	buf, err = encodeReadyForQuery(buf)
	if err != nil {
		return buf, err
	}

	return buf, nil
}

func handleTerminate(msg pgproto3.FrontendMessage) ([]byte, error) {
	return nil, nil
}
