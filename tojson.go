package godb

import (
	"database/sql"
	"errors"

	"github.com/btm6084/gojson"
)

var hex = "0123456789abcdef"

// ToJSON extracts a given SQL Rows result as json.
func ToJSON(rows *sql.Rows) ([]byte, error) {
	if rows == nil {
		return nil, errors.New("empty result set")
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

		first := true
		for k, v := range data {
			if len(v) == 0 {
				continue
			}

			if !first && k < len(data) {
				r = append(r, ',')
			}

			first = false

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

			// Encode the string to be valid JSON
			for _, b := range v {
				if b == '"' {
					r = append(r, []byte{'\\', '"'}...)
					continue
				}
				if b == '\\' {
					r = append(r, []byte{'\\', '\\'}...)
					continue
				}

				if b >= '\u0000' && b <= '\u001F' {
					r = append(r, []byte{'\\', 'u', '0', '0'}...)
					r = append(r, hex[b>>4])
					r = append(r, hex[b&0xF])
					continue
				}

				r = append(r, b)
			}

			r = append(r, '"')
		}

		r = append(r, '}')
		buf = append(buf, r...)
	}

	buf = append(buf, ']')
	return buf, nil
}
