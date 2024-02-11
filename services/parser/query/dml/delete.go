package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
)

/*
DELETE FROM <tableName>
[WHERE_INDEX <indexName> <condition> [AND <condition>]]
[WHERE <...condition>];
*/
type QueryDelete struct {
	query.Query
	DB         string                    `json:"db"`
	Table      string                    `json:"table"`
	Where      *statement.WhereStatement `json:"where"`
	WhereIndex *whereIndex               `json:"where_index"`
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
	qd.parseWhereIndex(s)
	qd.parseWhere(s)

	return nil
}

func (qd *QueryDelete) parseFrom(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "FROM" {
		panic(errors.ErrSyntax)
	}

	tok := s.Scan()
	word := s.TokenText()
	_, isKW := kwords.KeyWords[word]
	if tok == scanner.EOF {
		panic(errors.ErrSyntax)
	} else if isKW {
		panic(errors.ErrNoFrom)
	}

	qd.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, idKW := kwords.KeyWords[word]
	if tok != scanner.EOF && !idKW {
		panic(errors.ErrSyntax)
	}
}

func (qd *QueryDelete) parseWhereIndex(s *scanner.Scanner) {
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
	_, isKW = kwords.KeyWords[word]
	if tok == scanner.EOF || isKW {
		panic(errors.ErrSyntax)
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
	word := s.TokenText()
	if word != "WHERE" {
		return
	}

	qd.Where = parseWhere(s)
}
