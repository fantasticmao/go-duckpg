package duckpg

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/marcboeker/go-duckdb/v2"
	"log"
	"net"
	"os"
	"reflect"
)

var infoLog = log.New(os.Stdout, "[go-duckdb] ", log.Ldate|log.Ltime)
var errorLog = log.New(os.Stderr, "[go-duckdb] ", log.Lshortfile)

// Startup initializes the PostgreSQL wire server and listens for incoming connections.
func Startup(duckdb *sql.DB, address string) error {
	initDatabase(duckdb)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		infoLog.Printf("connection accepted, remote addr: %s\n", conn.RemoteAddr())

		go func(_conn net.Conn) {
			pgWire := &pgWire{
				conn:    _conn,
				backend: pgproto3.NewBackend(_conn, _conn),
			}

			err = pgWire.run()
			if err != nil {
				errorLog.Printf("pgwire server run error: %s\n", err.Error())
			}

			err = pgWire.close()
			if err != nil {
				errorLog.Printf("pgwire server close error: %s\n", err.Error())
			}
			infoLog.Printf("connection closed, remote addr: %s\n", _conn.RemoteAddr())
		}(conn)
	}
}

type pgWire struct {
	conn    net.Conn
	backend *pgproto3.Backend
}

func (pg *pgWire) run() error {
	err := pg.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := pg.backend.Receive()
		if err != nil {
			return fmt.Errorf("receive message error: %w", err)
		}

		msgType := reflect.TypeOf(msg)
		msgHandleFunc, ok := handlerMapping[msgType]
		if !ok {
			return fmt.Errorf("unsupported message: %#v", msg)
		}

		buf, err := msgHandleFunc(msg)
		if err != nil {
			return fmt.Errorf("handle message failed: %w", err)
		}
		if buf == nil {
			// Terminate
			return nil
		}

		_, err = pg.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("write response buffer error: %w", err)
		}
	}
}

func (pg *pgWire) handleStartup() error {
	msg, err := pg.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("receive startup message error: %w", err)
	}

	msgType := reflect.TypeOf(msg)
	msgHandleFunc, ok := handlerMapping[msgType]
	if !ok {
		return fmt.Errorf("unknown startup message: %#v", msg)
	}

	buf, err := msgHandleFunc(msg)
	if err != nil || buf == nil {
		return fmt.Errorf("handle startup message failed: %w", err)
	}

	_, err = pg.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("write response buffer error: %w", err)
	}

	if _, ok := msg.(*pgproto3.SSLRequest); ok {
		return pg.handleStartup()
	} else {
		return nil
	}
}

func (pg *pgWire) close() error {
	return pg.conn.Close()
}
