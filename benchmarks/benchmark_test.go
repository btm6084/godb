package benchmarks

import (
	"bytes"
	"testing"
)

var data = [][]byte{
	[]byte(`53550`),
	[]byte(`Free games of the week`),
	[]byte(`868e81aedec16378f797da17607d84b1`),
	[]byte(`http://www.pcgamer.com/free-games-of-the-week`),
	[]byte(`http://www.pcgamer.com/free-games-of-the-week`),
	[]byte(`c25717b1fa8205d2011a13e4763c15d78794cb2a`),
	[]byte(`The best of Ludum Dare, plus some other great stuff.`),
	[]byte(`<!-- ><iframe src="http://www.pcgamer.com/free-games-of-the-week" class="fgr_iframe" sandbox></iframe>< -->`),
	[]byte(`Sat, 28 May 2016 16:00:32 +0000`),
	[]byte(`03 - PC Gamer`),
	[]byte(`1464451232`),
	[]byte(`1`),
	[]byte(`281362`),
	[]byte(`Reaching people on the internet`),
	[]byte(`877ab1189c6ae5bcf5c9529fce7b7ac1`),
	[]byte(`http://theoatmeal.com/comics/reaching_people`),
	[]byte(`http://theoatmeal.com/comics/reaching_people`),
	[]byte(`4bfcbb326983dbbfd65336e30983d4dfaacb0a1f`),
	[]byte(`<a href="http://theoatmeal.com/comics/reaching_people"><img src="http://s3.amazonaws.com/theoatmeal-img/thumbnails/reaching_people.png" alt="Reaching people on the internet" class="border0" /></a><p></p><a href="http://theoatmeal.com/comics/reaching_people">View</a>`),
	[]byte(`<!-- ><iframe src="http://theoatmeal.com/comics/reaching_people" class="fgr_iframe" sandbox></iframe>< -->`),
	[]byte(`2017-10-25T18:29:55+01:00`),
	[]byte(`05 - The Oatmeal`),
	[]byte(`1508952595`),
	[]byte(`1`),
}

var cols = []string{
	`id`,
	`title`,
	`guid`,
	`link`,
	`target`,
	`target_hash`,
	`description`,
	`content`,
	`pubDate`,
	`source`,
	`timestamp`,
	`read`,
	`id`,
	`title`,
	`guid`,
	`link`,
	`target`,
	`target_hash`,
	`description`,
	`content`,
	`pubDate`,
	`source`,
	`timestamp`,
	`read`,
}

var types = []string{
	`INT`,
	`TEXT`,
	`VARCHAR`,
	`VARCHAR`,
	`TEXT`,
	`VARCHAR`,
	`TEXT`,
	`TEXT`,
	`VARCHAR`,
	`VARCHAR`,
	`INT`,
	`TINYINT`,
	`INT`,
	`TEXT`,
	`VARCHAR`,
	`VARCHAR`,
	`TEXT`,
	`VARCHAR`,
	`TEXT`,
	`TEXT`,
	`VARCHAR`,
	`VARCHAR`,
	`INT`,
	`TINYINT`,
}

func BenchmarkToJSONSumFirst(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// N-1 for each element separator, 2 for Open/Close object.
		sum := len(cols) + 1
		for k, v := range data {
			// Len(v) for each byte in the data, 2 for each key double quote, plus one for colon.
			sum += len(v) + len(cols[k]) + 3

			// 2 for each non-numeric value double quote.
			if !isMySQLNumeric(types[k]) {
				sum += 2

				// One for each quote escape sequence
				if bytes.Count(v, []byte{'"'}) > 0 {
					for _, c := range v {
						if c == '"' {
							sum++
						}
					}
				}
			}
		}

		r := make([]byte, sum)
		r[0] = '{'
		pos := 1

		for k, v := range data {
			r[pos] = '"'
			pos++

			copy(r[pos:], []byte(cols[k]))
			pos += len(cols[k])

			r[pos] = '"'
			pos++

			r[pos] = ':'
			pos++

			switch isMySQLNumeric(types[k]) {
			case true:
				copy(r[pos:], []byte(v))
				pos += len(v)
			case false:
				r[pos] = '"'
				pos++

				if bytes.Index(v, []byte{'"'}) >= 0 {
					for _, c := range v {
						if c == '"' {
							r[pos] = '\\'
							pos++
						}

						r[pos] = c
						pos++
					}
				} else {
					copy(r[pos:], []byte(v))
					pos += len(v)
				}

				r[pos] = '"'
				pos++
			}

			if k < len(data)-1 {
				r[pos] = ','
				pos++
			}
		}

		r[pos] = '}'
	}
}

func BenchmarkToJSONApend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := []byte{'{'}
		for k, v := range data {
			r = append(r, '"')
			r = append(r, cols[k]...)
			r = append(r, '"')
			r = append(r, ':')

			switch isMySQLNumeric(types[k]) {
			case true:
				r = append(r, v...)
			case false:
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
	}
}

func isMySQLNumeric(t string) bool {
	return t == `INT` || t == `TINYINT` || t == `SMALLINT` || t == `FLOAT` ||
		t == `DOUBLE` || t == `INTEGER` || t == `MEDIUMINT` || t == `BIGINT` ||
		t == `DECIMAL` || t == `NUMERIC` || t == `BIT`
}
