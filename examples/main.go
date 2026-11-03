package main

import (
	"database/sql"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/fantasticmao/go-duckpg/duckpg"
)

func main() {
	db, err := sql.Open("duckdb", "test.db")
	checkError(err)

	err = db.Ping()
	checkError(err)

	err = duckpg.Listen(db, ":15432", duckpg.Config{
		Users:         map[string]string{"duckpg": "123456"},
		TLS:           nil,
		ServerVersion: "15.0",
	})
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
