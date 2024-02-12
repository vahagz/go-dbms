package dml

import (
	"bytes"
	"encoding/json"
	"fmt"
	r "math/rand"
	"text/scanner"
	"time"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/dml/aggregator"
	"go-dbms/services/parser/query/dml/function"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"
)

var rand = r.NewSource(time.Now().UnixNano())

/*
SELECT <...projection>
FROM <tableName>
[WHERE_INDEX <indexName> <condition> [AND <condition>]]
[WHERE <...condition>]
[GROUP BY <...projection>];
*/
type QuerySelect struct {
	query.Query
	Projections *projection.Projections
	DB          string
	Table       string
	Where       *statement.WhereStatement
	WhereIndex  *whereIndex
	GroupBy     map[string]struct{}
}

func (qs *QuerySelect) Parse(s *scanner.Scanner) (err error) {
	defer func ()  {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	qs.Type = query.SELECT

	qs.parseProjections(s)
	qs.parseFrom(s)
	qs.parseWhereIndex(s)
	qs.parseWhere(s)
	qs.parseGroupBy(s)

	return nil
}

func (qs *QuerySelect) parseProjections(s *scanner.Scanner) {
	qs.Projections = projection.New()
	s.Scan()

	p := qs.parseProjection(s)
	qs.Projections.Add(p)

	for s.TokenText() != "FROM" {
		s.Scan()
		p = qs.parseProjection(s)
		qs.Projections.Add(p)
	}
}

func (qs *QuerySelect) parseProjection(s *scanner.Scanner) *projection.Projection {
	word := s.TokenText()
	_, isKW := kwords.KeyWords[word]
	if isKW || word == "," || word == "(" || word == ")" {
		panic(errors.ErrSyntax)
	}

	p := &projection.Projection{}
	p.Alias = word
	p.Name = word
	p.Type = projection.IDENTIFIER

	jsonVal, isLiteral := helpers.ParseJSONToken([]byte(word))

	s.Scan()
	word = s.TokenText()

	if isLiteral {
		p.Type = projection.LITERAL
		p.Literal = types.ParseJSONValue(jsonVal)
	} else if word == "FROM" || word == "," || word == ")" {
		return p
	} else if word == "(" {
		buf := bytes.NewBuffer([]byte(p.Alias))
		p.Arguments = []*projection.Projection{}

		if aggregator.IsAggregator(p.Name) {
			p.Type = projection.AGGREGATOR
		} else if function.IsFunction(p.Name) {
			p.Type = projection.FUNCTION
		} else {
			panic(fmt.Errorf("unknown aggregation/function: '%s'", p.Name))
		}

		buf.WriteByte('(')
		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			word = s.TokenText()
			if word == "," {
				continue
			} else if word == ")" {
				break
			}

			p.Arguments = append(p.Arguments, qs.parseProjection(s))

			buf.Write([]byte(word))
			buf.WriteByte(',')

			word := s.TokenText()
			if word == ")" {
				break
			}
		}

		buf.Truncate(buf.Len() - 1)
		buf.WriteByte(')')
		p.Alias = buf.String()

		s.Scan()
		word = s.TokenText()
	} else if word != "AS" {
		panic(errors.ErrSyntax)
	}

	if word == "AS" {
		s.Scan()
		p.Alias = s.TokenText()
		s.Scan()
	} else if p.Type == projection.LITERAL {
		p.Alias = fmt.Sprint(rand.Int63())
	}

	return p
}

func (qs *QuerySelect) parseFrom(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "FROM" {
		panic(errors.ErrNoFrom)
	}

	tok := s.Scan()
	if tok == scanner.EOF {
		panic(errors.ErrSyntax)
	}

	qs.Table = s.TokenText()
	s.Scan()
}

func (qs *QuerySelect) parseWhereIndex(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "WHERE_INDEX" {
		return
	}

	tok := s.Scan()
	word = s.TokenText()
	_, isKW := kwords.KeyWords[word]
	if tok == scanner.EOF || isKW {
		panic(errors.ErrSyntax)
	}

	qs.WhereIndex = &whereIndex{}
	qs.WhereIndex.Name = word
	qs.WhereIndex.FilterStart = &indexFilter{}
	col, op, val := parseWhereFilter(s, false)
	var valInt interface{}
	if err := json.Unmarshal([]byte(val), &valInt); err != nil {
		panic(err)
	}
	qs.WhereIndex.FilterStart.Operator = op
	qs.WhereIndex.FilterStart.Value = map[string]types.DataType{
		col: types.ParseJSONValue(valInt),
	}

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = kwords.KeyWords[word]
	if tok == scanner.EOF || isKW {
		panic(errors.ErrSyntax)
	}

	if word == "AND" {
		qs.WhereIndex.FilterEnd = &indexFilter{}
		col, op, val := parseWhereFilter(s, false)
		var valInt interface{}
		if err := json.Unmarshal([]byte(val), &valInt); err != nil {
			panic(err)
		}
		qs.WhereIndex.FilterEnd.Operator = op
		qs.WhereIndex.FilterEnd.Value = map[string]types.DataType{
			col: types.ParseJSONValue(valInt),
		}
	}

	s.Scan()
}

func parseWhereFilter(s *scanner.Scanner, firstScanned bool) (col, op, val string) {
	var tok rune
	var word string
	var isKW bool

	if !firstScanned {
		tok = s.Scan()
		word = s.TokenText()
		_, isKW = kwords.KeyWords[word]
		if tok == scanner.EOF || isKW {
			panic(errors.ErrSyntax)
		}
		col = word
	} else {
		col = s.TokenText()
	}
	
	tok = s.Scan()
	word = s.TokenText()
	_, isLO := kwords.IndexOperators[word]
	if tok == scanner.EOF || !isLO{
		panic(errors.ErrSyntax)
	}
	op = word
	
	if s.Peek() == '=' {
		op += "="
		s.Next()
	}

	tok = s.Scan()
	val = s.TokenText()
	if tok == scanner.EOF{
		panic(errors.ErrSyntax)
	}

	return col, op, val
}

func (qs *QuerySelect) parseWhere(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "WHERE" {
		return
	}

	qs.Where = parseWhere(s)
}

func parseWhere(s *scanner.Scanner) *statement.WhereStatement {
	var logOp string
	sttmnts := []*statement.WhereStatement{}

	for {
		tok := s.Scan()
		word := s.TokenText()
		_, isKW := kwords.KeyWords[word]
		if tok == scanner.EOF {
			panic(errors.ErrSyntax)
		} else if word == "(" {
			sttmnts = append(sttmnts, parseWhere(s))
		} else if word == ")" || word == ";" || word == "GROUP" || isKW {
			break
		} else if _, ok := kwords.LogicalOperators[word]; ok {
			logOp = word
		} else {
			col, op, val := parseWhereFilter(s, true)
			var valInt interface{}
			if err := json.Unmarshal([]byte(val), &valInt); err != nil {
				panic(err)
			}

			sttmnts = append(sttmnts, &statement.WhereStatement{
				Statement: &statement.Statement{
					Col: col,
					Op:  op,
					Val: types.ParseJSONValue(valInt),
				},
			})
		}
	}

	if logOp == "AND" {
		return &statement.WhereStatement{
			And: sttmnts,
		}
	} else if logOp == "OR" {
		return &statement.WhereStatement{
			Or: sttmnts,
		}
	}
	return sttmnts[0]
}

func (qs *QuerySelect) parseGroupBy(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "GROUP" {
		return
	}

	s.Scan()
	word = s.TokenText()
	if word != "BY" {
		panic(errors.ErrSyntax)
	}

	qs.GroupBy = map[string]struct{}{}
	for s.Scan(); s.TokenText() != ","; s.Scan() {
		qs.GroupBy[s.TokenText()] = struct{}{}

		s.Scan()
		word = s.TokenText()
		if word == "," {
			continue
		} else if word == ";" {
			break
		}
	}
}
