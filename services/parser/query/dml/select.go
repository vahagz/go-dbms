package dml

import (
	"bytes"
	"fmt"
	r "math/rand"
	"text/scanner"
	"time"

	"go-dbms/pkg/index"
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
	UseIndex    string
	Where       *statement.WhereStatement
	WhereIndex  *WhereIndex
	GroupBy     map[string]struct{}
}

func (qs *QuerySelect) Parse(s *scanner.Scanner) (err error) {
	defer helpers.RecoverOnError(&err)()

	qs.Type = query.SELECT

	qs.parseProjections(s)
	qs.parseFrom(s)
	qs.parseUseIndex(s)
	qs.parseWhereIndex(s)
	qs.parseWhere(s)
	qs.parseGroupBy(s)

	return nil
}

func (qs *QuerySelect) parseProjections(s *scanner.Scanner) {
	qs.Projections = projection.New()
	s.Scan()

	p := parseProjection(s)
	qs.Projections.Add(p)

	for s.TokenText() != "FROM" {
		s.Scan()
		p = parseProjection(s)
		qs.Projections.Add(p)
	}
}

func parseProjection(s *scanner.Scanner) *projection.Projection {
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
	_, isOP := kwords.IndexOperators[types.Operator(word)]

	if isLiteral {
		p.Type = projection.LITERAL
		p.Literal = types.ParseJSONValue(jsonVal)
		p.Alias = fmt.Sprint(rand.Int63())
		p.Name = p.Alias
	} else if word == "FROM" || word == "," || word == ")" || isOP {
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

			p.Arguments = append(p.Arguments, parseProjection(s))

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

func (qs *QuerySelect) parseUseIndex(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "USE_INDEX" {
		return
	}

	tok := s.Scan()
	if tok == scanner.EOF {
		panic(errors.ErrSyntax)
	}

	qs.UseIndex = s.TokenText()
	s.Scan()
}

func (qs *QuerySelect) parseWhereIndex(s *scanner.Scanner) {
	qs.WhereIndex = parseWhereIndex(s)
}

func parseWhereIndex(s *scanner.Scanner) *WhereIndex {
	word := s.TokenText()
	if word != "WHERE_INDEX" {
		return nil
	}

	s.Scan()
	if s.TokenText() != "(" {
		panic(errors.ErrSyntax)
	}

	wi := &WhereIndex{
		FilterStart: parseWhereIndexSection(s),
	}

	word = s.TokenText()
	if word == "AND" {
		s.Scan()
		if s.TokenText() != "(" {
			panic(errors.ErrSyntax)
		}

		wi.FilterEnd = parseWhereIndexSection(s)
	} else {
		s.Scan()
	}

	return wi
}

func parseWhereIndexSection(s *scanner.Scanner) *index.Filter {
	left, op, right := parseWhereFilter(s, false)
	f := &index.Filter{
		Operator: op,
		Conditions: []index.FilterCondition{{
			Left:  left,
			Right: right,
		}},
	}

	if s.TokenText() != ")" {
		for s.TokenText() == "AND" {
			left, _, right := parseWhereFilter(s, false)
			f.Conditions = append(f.Conditions, index.FilterCondition{
				Left:  left,
				Right: right,
			})
		}
	}

	if s.TokenText() != ")" {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	return f
}

func parseWhereFilter(s *scanner.Scanner, firstScanned bool) (
	left *projection.Projection,
	op types.Operator,
	right *projection.Projection,
) {
	if !firstScanned {
		s.Scan()
	}
	left = parseProjection(s)

	op = types.Operator(s.TokenText())
	_, isOP := kwords.IndexOperators[op]
	if !isOP {
		panic(errors.ErrSyntax)
	}

	if s.Peek() == '=' {
		op += "="
		s.Next()
	}

	s.Scan()
	right = parseProjection(s)

	return left, op, right
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
	var tok rune
	sttmnts := []*statement.WhereStatement{}

	tok = s.Scan()
	for {
		word := s.TokenText()
		_, isKW := kwords.KeyWords[word]
		if tok == scanner.EOF {
			panic(errors.ErrSyntax)
		} else if word == "(" {
			sttmnts = append(sttmnts, parseWhere(s))
			tok = s.Scan()
		} else if word == ")" || word == ";" || word == "GROUP" || isKW {
			break
		} else if _, ok := kwords.LogicalOperators[word]; ok {
			logOp = word
			tok = s.Scan()
		} else {
			left, op, right := parseWhereFilter(s, true)
			sttmnts = append(sttmnts, &statement.WhereStatement{
				Statement: &statement.Statement{
					Left:  left,
					Op:    op,
					Right: right,
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
