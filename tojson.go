package godb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/btm6084/gojson"
)

var hex = "0123456789abcdef"

// ToJSON extracts a given SQL Rows result as json.
func ToJSON(ctx context.Context, rows *sql.Rows) ([]byte, error) {
	var ctxErr error

	// We need to know if the context ends before we finish reading everything. If it does,
	// we have an unknown set and thus should consider the whole operation a failure.
	// Only do this if we have a deadline.
	go func() {
		if _, ok := ctx.Deadline(); !ok {
			return
		}

		<-ctx.Done()
		ctxErr = ctx.Err()
	}()

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

	i := 0
	for rows.Next() {
		err := rows.Scan(scan...)
		if err != nil {
			return nil, err
		}

		if ctxErr != nil {
			return nil, ctxErr
		}

		if i == 0 {
			buf.WriteByte('{')
			i++
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

	buf.WriteByte(']')

	if ctxErr != nil {
		return nil, ctxErr
	}

	return buf.Bytes(), nil
}
