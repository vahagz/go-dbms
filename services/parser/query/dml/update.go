package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
	"go-dbms/util/helpers"
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
	DB         string
	Table      string
	UseIndex   string
	Values     types.DataRow
	Where      *statement.WhereStatement
	WhereIndex *WhereIndex
}

func (qu *QueryUpdate) Parse(s *scanner.Scanner, ps query.Parser) (err error) {
	defer helpers.RecoverOnError(&err)()

	qu.Type = query.UPDATE

	qu.parseFrom(s)
	qu.parseUseIndex(s)
	qu.parseValues(s)
	qu.parseWhereIndex(s, ps)
	qu.parseWhere(s, ps)

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

func (qs *QueryUpdate) parseUseIndex(s *scanner.Scanner) {
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

func (qu *QueryUpdate) parseValues(s *scanner.Scanner) {
	qu.Values = types.DataRow{}

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

func (qs *QueryUpdate) parseWhereIndex(s *scanner.Scanner, ps query.Parser) {
	qs.WhereIndex = parseWhereIndex(s, ps)
}

func (qu *QueryUpdate) parseWhere(s *scanner.Scanner, ps query.Parser) {
	word := s.TokenText()
	if word != "WHERE" {
		return
	}

	qu.Where = parseWhere(s, ps)
}
