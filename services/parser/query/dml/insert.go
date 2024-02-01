package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
)

type QueryInsert struct {
	query.Query
	DB      string    `json:"db"`
	Table   string    `json:"table"`
	Columns []string  `json:"columns"`
	Values  []dataRow `json:"values"`
}

func (qi *QueryInsert) Parse(s *scanner.Scanner) (err error) {
	defer func ()  {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	qi.Type = query.INSERT

	qi.parseInto(s)
	qi.parseColumns(s)
	qi.parseValues(s)

	return nil
}

func (qi *QueryInsert) parseInto(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "INTO" {
		panic(ErrSyntax)
	}

	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF {
		panic(ErrSyntax)
	} else if isKW {
		panic(ErrNoFrom)
	}

	qi.Table = word

	tok = s.Scan()
	word = s.TokenText()
	if tok != scanner.EOF && word != "(" {
		panic(ErrSyntax)
	}
}

func (qi *QueryInsert) parseColumns(s *scanner.Scanner) {
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		_, isKW := keyWords[word]
		if tok == scanner.EOF || isKW {
			panic(ErrSyntax)
		}

		qi.Columns = append(qi.Columns, word)

		tok = s.Scan()
		word = s.TokenText()
		_, isKW = keyWords[word]
		if tok == scanner.EOF || (word != "," && word != ")") || isKW {
			panic(ErrSyntax)
		} else if word == ")" {
			return
		}
	}
}

func (qi *QueryInsert) parseValues(s *scanner.Scanner) {
	qi.Values = []dataRow{}

	s.Scan()
	if s.TokenText() != "VALUES" {
		panic(ErrSyntax)
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		row := dataRow{}

		if word != "(" {
			panic(ErrSyntax)
		}

		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			word := s.TokenText()
			_, isKW := keyWords[word]
			if tok == scanner.EOF || isKW {
				panic(ErrSyntax)
			}

			var val interface{}
			if err := json.Unmarshal([]byte(word), &val); err != nil {
				panic(err)
			}
			row = append(row, types.ParseJSONValue(val))

			tok = s.Scan()
			word = s.TokenText()
			_, isKW = keyWords[word]
			if tok == scanner.EOF || (word != "," && word != ")") || isKW {
				panic(ErrSyntax)
			} else if word == ")" {
				break
			}
		}

		qi.Values = append(qi.Values, row)

		s.Scan()
		word = s.TokenText()
		if word != "," && word != ";" {
			panic(ErrSyntax)
		}
	}
}
