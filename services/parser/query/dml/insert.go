package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
	"go-dbms/util/helpers"
)

/*
INSERT INTO <tableName> (...columns)
VALUES
	(...values)
	...
	(...values);
*/
type QueryInsert struct {
	query.Query
	DB      string
	Table   string
	Columns []string
	Values  [][]types.DataType
}

func (qi *QueryInsert) Parse(s *scanner.Scanner, ps query.Parser) (err error) {
	defer helpers.RecoverOnError(&err)()

	qi.Type = query.INSERT

	qi.parseInto(s)
	qi.parseColumns(s)
	qi.parseValues(s)

	return nil
}

func (qi *QueryInsert) parseInto(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "INTO" {
		panic(errors.ErrSyntax)
	}

	tok := s.Scan()
	word := s.TokenText()
	_, isKW := kwords.KeyWords[word]
	if tok == scanner.EOF || isKW {
		panic(errors.ErrSyntax)
	}

	qi.Table = word

	tok = s.Scan()
	word = s.TokenText()
	if tok != scanner.EOF && word != "(" {
		panic(errors.ErrSyntax)
	}
}

func (qi *QueryInsert) parseColumns(s *scanner.Scanner) {
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		_, isKW := kwords.KeyWords[word]
		if tok == scanner.EOF || isKW {
			panic(errors.ErrSyntax)
		}

		qi.Columns = append(qi.Columns, word)

		tok = s.Scan()
		word = s.TokenText()
		_, isKW = kwords.KeyWords[word]
		if tok == scanner.EOF || (word != "," && word != ")") || isKW {
			panic(errors.ErrSyntax)
		} else if word == ")" {
			return
		}
	}
}

func (qi *QueryInsert) parseValues(s *scanner.Scanner) {
	qi.Values = [][]types.DataType{}

	s.Scan()
	if s.TokenText() != "VALUES" {
		panic(errors.ErrSyntax)
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		row := []types.DataType{}

		if word != "(" {
			panic(errors.ErrSyntax)
		}

		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			word := s.TokenText()
			_, isKW := kwords.KeyWords[word]
			if tok == scanner.EOF || isKW {
				panic(errors.ErrSyntax)
			}

			var val interface{}
			if err := json.Unmarshal([]byte(word), &val); err != nil {
				panic(err)
			}
			row = append(row, types.ParseJSONValue(val))

			tok = s.Scan()
			word = s.TokenText()
			_, isKW = kwords.KeyWords[word]
			if tok == scanner.EOF || (word != "," && word != ")") || isKW {
				panic(errors.ErrSyntax)
			} else if word == ")" {
				break
			}
		}

		qi.Values = append(qi.Values, row)

		s.Scan()
		word = s.TokenText()
		if word != "," && word != ";" {
			panic(errors.ErrSyntax)
		}
	}
}
