package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
)

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

	word := s.TokenText()
	if word == "WHERE_INDEX" {
		qu.parseWhereIndex(s)
	}

	word = s.TokenText()
	if word == "WHERE" {
		qu.parseWhere(s)
	}

	return nil
}

func (qu *QueryUpdate) parseFrom(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF {
		panic(ErrSyntax)
	} else if isKW {
		panic(ErrNoFrom)
	}

	qu.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, idKW := keyWords[word]
	if tok != scanner.EOF && !idKW {
		panic(ErrSyntax)
	}
}

func (qu *QueryUpdate) parseValues(s *scanner.Scanner) {
	qu.Values = dataMap{}

	if s.TokenText() != "SET" {
		panic(ErrSyntax)
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		col := s.TokenText()
		_, isKW := keyWords[col]
		if tok == scanner.EOF || isKW {
			panic(ErrSyntax)
		}

		tok = s.Scan()
		if tok == scanner.EOF || s.TokenText() != "=" {
			panic(ErrSyntax)
		}

		tok = s.Scan()
		val := s.TokenText()
		_, isKW = keyWords[val]
		if tok == scanner.EOF || isKW {
			panic(ErrSyntax)
		}

		var valInt interface{}
		if err := json.Unmarshal([]byte(val), &valInt); err != nil {
			panic(err)
		}
		qu.Values[col] = types.ParseJSONValue(valInt)

		s.Scan()
		word := s.TokenText()
		_, isKW = keyWords[word]
		if word == ";" || isKW {
			break
		} else if word == "," {
			continue
		} else {
			panic(ErrSyntax)
		}
	}
}

func (qu *QueryUpdate) parseWhereIndex(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
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
	_, isKW = keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
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
	qu.Where = (*where)(parseWhere(s))
}
