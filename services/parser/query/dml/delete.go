package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
)

type QueryDelete struct {
	query.Query
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}

func (qd *QueryDelete) Parse(s *scanner.Scanner) (err error) {
	defer func ()  {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	qd.Type = query.DELETE

	qd.parseFrom(s)

	word := s.TokenText()
	if word == "WHERE_INDEX" {
		qd.parseWhereIndex(s)
	}

	word = s.TokenText()
	if word == "WHERE" {
		qd.parseWhere(s)
	}

	return nil
}

func (qd *QueryDelete) parseFrom(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF {
		panic(ErrSyntax)
	} else if isKW {
		panic(ErrNoFrom)
	}

	qd.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, idKW := keyWords[word]
	if tok != scanner.EOF && !idKW {
		panic(ErrSyntax)
	}
}

func (qd *QueryDelete) parseWhereIndex(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
	}

	qd.WhereIndex = &whereIndex{}
	qd.WhereIndex.Name = word
	qd.WhereIndex.FilterStart = &indexFilter{}
	col, op, val := parseWhereFilter(s, false)
	var valInt interface{}
	if err := json.Unmarshal([]byte(val), &valInt); err != nil {
		panic(err)
	}
	qd.WhereIndex.FilterStart.Operator = op
	qd.WhereIndex.FilterStart.Value = map[string]types.DataType{
		col: types.ParseJSONValue(valInt),
	}

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
	}

	if word == "AND" {
		qd.WhereIndex.FilterEnd = &indexFilter{}
		col, op, val := parseWhereFilter(s, false)
		var valInt interface{}
		if err := json.Unmarshal([]byte(val), &valInt); err != nil {
			panic(err)
		}
		qd.WhereIndex.FilterEnd.Operator = op
		qd.WhereIndex.FilterEnd.Value = map[string]types.DataType{
			col: types.ParseJSONValue(valInt),
		}
	}

	s.Scan()
}

func (qd *QueryDelete) parseWhere(s *scanner.Scanner) {
	qd.Where = (*where)(parseWhere(s))
}
