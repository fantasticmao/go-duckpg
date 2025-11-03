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

	err = duckpg.Startup(db, ":5432")
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
