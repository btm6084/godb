package godb

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
)

type Dialect string
type Join string

const (
	DialectMSSQL    Dialect = "mssql"
	DialectPostgres Dialect = "postgresql"
)

var (
	dialectMap = map[Dialect]string{
		DialectMSSQL:    "@p",
		DialectPostgres: "$",
	}
)

type builder struct {
	Dialect Dialect

	Where []string
	Args  []interface{}
}

func NewBuilder(d Dialect) *builder {
	return &builder{Dialect: d}
}

func boundParameter(d Dialect, n int) string {
	return dialectMap[d] + cast.ToString(n)
}

// BuildWhere just joins all the where strings. Join should be AND or OR
func (b *builder) BuildWhere(join string) string {
	if len(b.Where) < 1 {
		return ""
	}

	join = strings.ToLower(join)
	if join != "and" && join != "or" {
		join = "AND"
	}
	join = strings.ToUpper(join)

	return "WHERE (" + strings.Join(b.Where, ") "+join+" (") + ")"
}

// WhereDistinct adds to the where clause a term that requires the value in the given field to be distinct from the given value.
// Note that depending on your use case, you may need to cast your val to be what you expect.
// e.g. casting a bool to an int.
func (b *builder) WhereDistinct(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	bp := boundParameter(b.Dialect, len(b.Args))

	switch b.Dialect {
	case DialectMSSQL:
		// ((a <> b OR a IS NULL OR b IS NULL) AND NOT (a IS NULL AND b IS NULL))
		b.Where = append(b.Where, fmt.Sprintf(`((%s <> %s OR %s IS NULL OR %s IS NULL) AND NOT (%s IS NULL AND %s IS NULL))`, field, bp, field, bp, field, bp))
	case DialectPostgres:
		b.Where = append(b.Where, fmt.Sprintf(`%s IS DISTINCT FROM %s`, field, bp))
	}

	return b
}

// WhereNotDistinct adds to the where clause a term that requires the value in the given field to be NOT distinct from the given value.
// Note that depending on your use case, you may need to cast your val to be what you expect.
// e.g. casting a bool to an int.
func (b *builder) WhereNotDistinct(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	bp := boundParameter(b.Dialect, len(b.Args))

	switch b.Dialect {
	case DialectMSSQL:
		// (NOT (a <> b OR a IS NULL OR b IS NULL) OR (a IS NULL AND b IS NULL))
		b.Where = append(b.Where, fmt.Sprintf(`(NOT (%s <> %s OR %s IS NULL OR %s IS NULL) OR (%s IS NULL AND %s IS NULL))`, field, bp, field, bp, field, bp))
	case DialectPostgres:
		b.Where = append(b.Where, fmt.Sprintf(`%s IS NOT DISTINCT FROM %s`, field, bp))
	}

	return b
}

// WhereNull adds to the where clause a term that requires the value in the given field is null.
func (b *builder) WhereNull(field string) *builder {
	b.Where = append(b.Where, fmt.Sprintf(`%s IS NULL`, field))
	return b
}

// WhereNotNull adds to the where clause a term that requires the value in the given field is not null.
func (b *builder) WhereNotNull(field string) *builder {
	b.Where = append(b.Where, fmt.Sprintf(`%s <> ''`, field))
	return b
}

// WhereLess adds to the where clause a term that requires the value in the given field is less than the value.
func (b *builder) WhereLess(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s < %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereLessEq adds to the where clause a term that requires the value in the given field is less than or equal to the value.
func (b *builder) WhereLessEq(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s <= %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereGreater adds to the where clause a term that requires the value in the given field is greater than the value.
func (b *builder) WhereGreater(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s > %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereGreaterEq adds to the where clause a term that requires the value in the given field is greater than or equal to the value.
func (b *builder) WhereGreaterEq(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s >= %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereExact adds to the where clause a term that requires the value in the given field to exactly match the given value.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql)
func (b *builder) WhereExact(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s = %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereExact adds to the where clause a term that requires the value in the given field to NOT exactly match the given.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql).
func (b *builder) WhereNotExact(field string, val interface{}) *builder {
	b.Args = append(b.Args, val)
	b.Where = append(b.Where, fmt.Sprintf(`%s != %s`, field, boundParameter(b.Dialect, len(b.Args))))
	return b
}

// WhereLike adds to the where clause a term that requires the value in the given field to at least partially match the values given.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql).
func (b *builder) WhereLike(field string, vals ...string) *builder {
	where := make([]string, len(vals))
	for _, val := range vals {
		val = "%" + val + "%"
		b.Args = append(b.Args, val)
		where = append(where, fmt.Sprintf(`%s LIKE %s`, field, boundParameter(b.Dialect, len(b.Args))))
	}

	b.Where = append(b.Where, "("+strings.Join(b.Where, ") OR (")+")")
	return b
}

// WhereNotLike adds to the where clause a term that requires that the value in the field does NOT partially match the values given.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql).
func (b *builder) WhereNotLike(field string, vals ...string) *builder {
	where := make([]string, len(vals))
	for _, val := range vals {
		val = "%" + val + "%"
		b.Args = append(b.Args, val)
		where = append(where, fmt.Sprintf(`%s NOT LIKE %s`, field, boundParameter(b.Dialect, len(b.Args))))
	}

	b.Where = append(b.Where, "("+strings.Join(b.Where, ") AND (")+")")
	return b
}

// WhereIn adds to the where clause a term that requires that the values in the given field exactly match one or more of the values given.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql).
func (b *builder) WhereIn(field string, vals ...interface{}) *builder {
	if len(vals) == 0 {
		return b
	}

	var ps []string
	for _, v := range vals {
		b.Args = append(b.Args, v)
		ps = append(ps, boundParameter(b.Dialect, len(b.Args)))
	}

	b.Where = append(b.Where, fmt.Sprintf(`%s IN (%s)`, field, strings.Join(ps, ",")))

	return b
}

// WhereNotIn adds to the where clause a term that requires that the values in the given field exactly match none of the values given.
// If you want to LOWER your field for matching, wrap the field name in LOWER() when passing it in (mssql, psql).
func (b *builder) WhereNotIn(field string, vals ...interface{}) *builder {
	if len(vals) == 0 {
		return b
	}

	var ps []string
	for _, v := range vals {
		b.Args = append(b.Args, v)
		ps = append(ps, boundParameter(b.Dialect, len(b.Args)))
	}

	b.Where = append(b.Where, fmt.Sprintf(`%s NOT IN (%s)`, field, strings.Join(ps, ",")))

	return b
}

// AddToArgs simply adds additional parameters to the Args array. This will have an impact on the bound parameter count of any subsequent WHERE clauses added.
// This is useful for things like subqueries where you're not directly matching against a single field.
func (b *builder) AddToArgs(vals ...interface{}) *builder {
	var ps []string
	for _, v := range vals {
		b.Args = append(b.Args, v)
		ps = append(ps, boundParameter(b.Dialect, len(b.Args)))
	}

	return b
}
