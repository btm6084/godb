package godb

import (
	"database/sql"

	"github.com/btm6084/gojson"
	"github.com/btm6084/utilities/metrics"
)

// Unmarshal extracts a given SQL Rows result into a given container.
func Unmarshal(rows *sql.Rows, v interface{}) error {
	j, err := ToJSON(rows)
	if err != nil {
		return err
	}

	return gojson.Unmarshal(j, v)
}

// UnmarshalWithMetrics extracts a given SQL Rows result into a given container.
func UnmarshalWithMetrics(r metrics.Recorder, rows *sql.Rows, v interface{}) error {
	end := r.Segment("GODB::UnmarshalWithMetrics::ToJSON")
	j, err := ToJSON(rows)
	end()
	if err != nil {
		return err
	}

	end = r.Segment("GODB::UnmarshalWithMetrics::GoJSON.Unmarshal")
	err = gojson.Unmarshal(j, v)
	end()

	return err
}
