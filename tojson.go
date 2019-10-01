package godb

import (
	"bytes"
	"database/sql"
	"errors"

	"github.com/btm6084/gojson"
)

// ToJSON extracts a given SQL Rows result as json.
func ToJSON(rows Rows) ([]byte, error) {
	if rows == nil {
		return nil, errors.New("Empty result set")
	}

	defer rows.Close()

	cols, err := rows.Columns()
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
			if len(v) == 0 {
				continue
			}

			if k != 0 && k < len(data) {
				r = append(r, ',')
			}

			r = append(r, '"')
			r = append(r, cols[k]...)
			r = append(r, '"')
			r = append(r, ':')

			// Don't quote or escape valid json.
			if gojson.IsJSON(v) {
				r = append(r, v...)
				continue
			}

			r = append(r, '"')
			if bytes.Count(v, []byte{'"'}) > 0 {
				r = append(r, bytes.Replace(v, []byte{'"'}, []byte{'\\', '"'}, -1)...)
			} else {
				r = append(r, v...)
			}
			r = append(r, '"')
		}

		r = append(r, '}')
		buf = append(buf, r...)
	}

	buf = append(buf, ']')
	return buf, nil
}
