package godb

import (
	"bytes"
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

	var buf bytes.Buffer
	buf.WriteByte('[')

	data := make([]sql.RawBytes, len(cols))
	scan := make([]interface{}, len(data))

	for i := range scan {
		scan[i] = &data[i]
	}

	for i := 0; rows.Next(); i++ {
		err := rows.Scan(scan...)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			buf.WriteByte('{')
		} else {
			buf.WriteString(`,{`)
		}

		first := true
		for k, v := range data {
			if len(v) == 0 {
				continue
			}

			if !first && k < len(data) {
				buf.WriteByte(',')
			}

			first = false

			buf.WriteByte('"')
			buf.WriteString(cols[k])
			buf.WriteString(`":`)

			// Don't quote or escape valid json.
			if gojson.IsJSON(v) {
				buf.WriteString(string(v))
				continue
			}

			buf.WriteByte('"')

			// Encode the string to be valid JSON
			for _, b := range v {
				if b == '"' {
					buf.WriteString(`\"`)
					continue
				}
				if b == '\\' {
					buf.WriteString(`\\`)
					continue
				}

				if b >= '\u0000' && b <= '\u001F' {
					buf.WriteString(`\u00`)
					buf.WriteByte(hex[b>>4])
					buf.WriteByte(hex[b&0xF])
					continue
				}

				buf.WriteByte(b)
			}

			buf.WriteByte('"')
		}

		buf.WriteByte('}')
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	buf.WriteByte(']')

	return buf.Bytes(), nil
}
