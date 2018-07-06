package godb

import (
	"database/sql"
	"log"

	"github.com/btm6084/gojson"
)

// Unmarshal extracts a given SQL Rows result into a given container.
func Unmarshal(rows *sql.Rows, v interface{}) error {
	j, err := ToJSON(rows)
	if err != nil {
		log.Println(err)
		return err
	}

	return gojson.Unmarshal(j, v)
}
