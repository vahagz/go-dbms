package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
)

/*
UPDATE <tableName>
SET
	<columnName> = <value>,
	...
	<columnName> = <value>
[WHERE_INDEX <indexName> <condition> [AND <condition>]]
[WHERE <...condition>];
*/
type QueryUpdate struct {
	query.Query
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Values     dataMap     `json:"values"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}

func (qu *QueryUpdate) Parse(s *scanner.Scanner) (err error) {
	defer func ()  {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	qu.Type = query.INSERT

	qu.parseFrom(s)
	qu.parseValues(s)
	qu.parseWhereIndex(s)
	qu.parseWhere(s)

	return nil
}

func (qu *QueryUpdate) parseFrom(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := kwords.KeyWords[word]
	if tok == scanner.EOF {
		panic(errors.ErrSyntax)
	} else if isKW {
		panic(errors.ErrNoFrom)
	}

	qu.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, idKW := kwords.KeyWords[word]
	if tok != scanner.EOF && !idKW {
		panic(errors.ErrSyntax)
	}
}

func (qu *QueryUpdate) parseValues(s *scanner.Scanner) {
	qu.Values = dataMap{}

	if s.TokenText() != "SET" {
		panic(errors.ErrSyntax)
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		col := s.TokenText()
		_, isKW := kwords.KeyWords[col]
		if tok == scanner.EOF || isKW {
			panic(errors.ErrSyntax)
		}

		tok = s.Scan()
		if tok == scanner.EOF || s.TokenText() != "=" {
			panic(errors.ErrSyntax)
		}

		tok = s.Scan()
		val := s.TokenText()
		_, isKW = kwords.KeyWords[val]
		if tok == scanner.EOF || isKW {
			panic(errors.ErrSyntax)
		}

		var valInt interface{}
		if err := json.Unmarshal([]byte(val), &valInt); err != nil {
			panic(err)
		}
		qu.Values[col] = types.ParseJSONValue(valInt)

		s.Scan()
		word := s.TokenText()
		_, isKW = kwords.KeyWords[word]
		if word == ";" || isKW {
			break
		} else if word == "," {
			continue
		} else {
			panic(errors.ErrSyntax)
		}
	}
}

func (qu *QueryUpdate) parseWhereIndex(s *scanner.Scanner) {
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

	qu.WhereIndex = &whereIndex{}
	qu.WhereIndex.Name = word
	qu.WhereIndex.FilterStart = &indexFilter{}
	col, op, val := parseWhereFilter(s, false)
	var valInt interface{}
	if err := json.Unmarshal([]byte(val), &valInt); err != nil {
		panic(err)
	}
	qu.WhereIndex.FilterStart.Operator = op
	qu.WhereIndex.FilterStart.Value = map[string]types.DataType{
		col: types.ParseJSONValue(valInt),
	}

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = kwords.KeyWords[word]
	if tok == scanner.EOF || isKW {
		panic(errors.ErrSyntax)
	}

	if word == "AND" {
		qu.WhereIndex.FilterEnd = &indexFilter{}
		col, op, val := parseWhereFilter(s, false)
		var valInt interface{}
		if err := json.Unmarshal([]byte(val), &valInt); err != nil {
			panic(err)
		}
		qu.WhereIndex.FilterEnd.Operator = op
		qu.WhereIndex.FilterEnd.Value = map[string]types.DataType{
			col: types.ParseJSONValue(valInt),
		}
	}

	s.Scan()
}

func (qu *QueryUpdate) parseWhere(s *scanner.Scanner) {
	word := s.TokenText()
	if word == "WHERE" {
		return
	}

	qu.Where = (*where)(parseWhere(s))
}
