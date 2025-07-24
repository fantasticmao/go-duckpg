package main

import (
	"database/sql"
	"github.com/fantasticmao/go-duckpg/duckpg"
	_ "github.com/marcboeker/go-duckdb/v2"
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
