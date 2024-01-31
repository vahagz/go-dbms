package dml

import (
	"encoding/json"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
	"text/scanner"

	"github.com/pkg/errors"
)

var (
	ErrSyntax       = errors.New("syntax error")
	ErrNoSelection  = errors.New("empty 'SELECT' list")
	ErrNoFrom       = errors.New("empty 'FROM' clause")
	ErrNoWhereIndex = errors.New("empty 'WHERE_INDEX' clause")
)

type QuerySelect struct {
	query.Query
	Columns    []string    `json:"columns"`
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
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

	qs.parseSelection(s)

	word := s.TokenText()
	if word != "FROM" {
		return ErrNoFrom
	}

	qs.parseFrom(s)

	word = s.TokenText()
	if word == "WHERE_INDEX" {
		qs.parseWhereIndex(s)
	}

	// switch word {
	// 	case ";": break
	// 	case "WHERE_INDEX":
	// 	case "WHERE":
	// 	case "GROUP_BY":
	// 	case "HAVING":
	// 	case "ORDER_BY":
	// 	case "LIMIT":
	// }

	return nil
}

func (qs *QuerySelect) parseSelection(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF {
		panic(ErrSyntax)
	} else if isKW {
		panic(ErrNoSelection)
	}

	qs.Columns = append(qs.Columns, word)

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = keyWords[word]
	if tok == scanner.EOF || (word != "," && !isKW) {
		panic(ErrSyntax)
	} else if isKW {
		return
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		_, isKW := keyWords[word]
		if word == "," {
			panic(ErrSyntax)
		} else if isKW {
			return
		}

		qs.Columns = append(qs.Columns, word)

		tok := s.Scan()
		word = s.TokenText()
		_, isKW = keyWords[word]
		if tok == scanner.EOF || (word != "," && !isKW) {
			panic(ErrSyntax)
		} else if isKW {
			return
		}
	}
}

func (qs *QuerySelect) parseFrom(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF {
		panic(ErrSyntax)
	} else if isKW {
		panic(ErrNoFrom)
	}

	qs.Table = word

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = keyWords[word]
	if tok != scanner.EOF && !isKW {
		panic(ErrSyntax)
	}
}

func (qs *QuerySelect) parseWhereIndex(s *scanner.Scanner) {
	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
	}

	qs.WhereIndex = &whereIndex{}
	qs.WhereIndex.Name = word
	qs.WhereIndex.FilterStart = parseWhereIndexFilter(s)

	tok = s.Scan()
	word = s.TokenText()
	_, isKW = keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
	}

	if word == "AND" {
		qs.WhereIndex.FilterEnd = parseWhereIndexFilter(s)
	}
}

func parseWhereIndexFilter(s *scanner.Scanner) *indexFilter {
	f := &indexFilter{}

	tok := s.Scan()
	word := s.TokenText()
	_, isKW := keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
	}
	col := word
	
	tok = s.Scan()
	word = s.TokenText()
	_, isLO := indexOperators[word]
	if tok == scanner.EOF || !isLO{
		panic(ErrSyntax)
	}
	op := word
	
	if s.Peek() == '=' {
		op += "="
		s.Next()
	}

	tok = s.Scan()
	word = s.TokenText()
	if tok == scanner.EOF{
		panic(ErrSyntax)
	}

	var val interface{}
	if err := json.Unmarshal([]byte(word), &val); err != nil {
		panic(err)
	}

	f.Operator = op
	f.Value = map[string]types.DataType{
		col: types.ParseJSONValue(val),
	}
	return f
}
