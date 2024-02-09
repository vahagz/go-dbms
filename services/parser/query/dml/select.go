package dml

import (
	"bytes"
	"encoding/json"
	"fmt"
	"text/scanner"

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

/*
SELECT <...columns>
FROM <tableName>
[WHERE_INDEX <indexName> <condition> [AND <condition>]]
[WHERE <...condition>];
*/
type QuerySelect struct {
	query.Query
	Projections map[string]*projection.Projection
	DB          string
	Table       string
	Where       *where
	WhereIndex  *whereIndex
	GroupBy     []string
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
	qs.Projections = map[string]*projection.Projection{}
	s.Scan()

	p := qs.parseProjection(s)
	qs.Projections[p.Alias] = p

	for s.TokenText() != "FROM" {
		s.Scan()
		p = qs.parseProjection(s)
		qs.Projections[p.Alias] = p
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

	s.Scan()
	word = s.TokenText()
	if word == "FROM" || word == "," || word == ")" || word == "AS" {
		if word == "AS" {
			s.Scan()
			p.Alias = s.TokenText()
			s.Scan()
		}
		return p
	} else if word != "(" {
		panic(errors.ErrSyntax)
	}

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

		jsonVal, ok := helpers.ParseJSONToken([]byte(word))
		if ok {
			p.Arguments = append(p.Arguments, &projection.Projection{
				Literal: types.ParseJSONValue(jsonVal),
			})
			s.Scan()
		} else {
			p.Arguments = append(p.Arguments, qs.parseProjection(s))
		}

		buf.Write([]byte(word))
		buf.WriteByte(',')

		word := s.TokenText()
		if word == ")" {
			s.Scan()
			word = s.TokenText()
			if word == "AS" {
				s.Scan()
				p.Alias = s.TokenText()
				s.Scan()
			}

			break
		}
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteByte(')')

	if p.Alias == "" {
		p.Alias = buf.String()
	}
	return p
}

func (qs *QuerySelect) parseFrom(s *scanner.Scanner) {
	word := s.TokenText()	
	_, isKW := kwords.KeyWords[word]
	if word != "FROM" || isKW {
		panic(errors.ErrNoFrom)
	}

	tok := s.Scan()
	if tok == scanner.EOF {
		panic(errors.ErrSyntax)
	}

	qs.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = kwords.KeyWords[word]
	if tok != scanner.EOF && !isKW {
		panic(errors.ErrSyntax)
	}
}

func (qs *QuerySelect) parseWhereIndex(s *scanner.Scanner) {
	word := s.TokenText()
	if word == "WHERE_INDEX" {
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
	if word == "WHERE" {
		return
	}

	qs.Where = (*where)(parseWhere(s))
}

func parseWhere(s *scanner.Scanner) (*statement.WhereStatement) {
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
	
}
