package godb

import (
	"bytes"
	"database/sql"
	"errors"

	"github.com/btm6084/gojson"
)

// ToJSON extracts a given SQL Rows result as json.
func ToJSON(rows *sql.Rows) ([]byte, error) {
	if rows == nil {
		return nil, errors.New("Empty result set")
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	buf := []byte{'['}

	data := make([]sql.RawBytes, len(cols))
	scan := make([]interface{}, len(data))

	for i := range scan {
		scan[i] = &data[i]
	}

	for i := 0; rows.Next(); i++ {
		rows.Scan(scan...)

		var r []byte

		if i == 0 {
			r = []byte{'{'}
		} else {
			r = []byte{',', '{'}
		}
		for k, v := range data {
			r = append(r, '"')
			r = append(r, cols[k]...)
			r = append(r, '"')
			r = append(r, ':')

			switch isMySQLNumeric(types[k].DatabaseTypeName()) {
			case true:
				if len(v) == 0 {
					r = append(r, []byte{'n', 'u', 'l', 'l'}...)
				} else {
					r = append(r, v...)
				}
			case false:
				// Don't quote or escape valid json.
				if gojson.IsJSON(v) {
					r = append(r, v...)
					break
				}

				r = append(r, '"')
				if bytes.Count(v, []byte{'"'}) > 0 {
					r = append(r, bytes.Replace(v, []byte{'"'}, []byte{'\\', '"'}, -1)...)
				} else {
					r = append(r, v...)
				}
				r = append(r, '"')
			}

			if k < len(data)-1 {
				r = append(r, ',')
			}
		}

		r = append(r, '}')
		buf = append(buf, r...)
	}

	buf = append(buf, ']')
	return buf, nil
}

func isMySQLNumeric(t string) bool {
	return t == `INT` || t == `TINYINT` || t == `SMALLINT` || t == `FLOAT` ||
		t == `DOUBLE` || t == `INTEGER` || t == `MEDIUMINT` || t == `BIGINT` ||
		t == `DECIMAL` || t == `NUMERIC` || t == `BIT`
}
