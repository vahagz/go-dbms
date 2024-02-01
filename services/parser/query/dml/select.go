package dml

import (
	"encoding/json"
	"text/scanner"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"

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

	word = s.TokenText()
	if word == "WHERE" {
		qs.parseWhere(s)
	}

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
	_, isKW = keyWords[word]
	if tok == scanner.EOF || isKW {
		panic(ErrSyntax)
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
		_, isKW = keyWords[word]
		if tok == scanner.EOF || isKW {
			panic(ErrSyntax)
		}
		col = word
	} else {
		col = s.TokenText()
	}
	
	tok = s.Scan()
	word = s.TokenText()
	_, isLO := indexOperators[word]
	if tok == scanner.EOF || !isLO{
		panic(ErrSyntax)
	}
	op = word
	
	if s.Peek() == '=' {
		op += "="
		s.Next()
	}

	tok = s.Scan()
	val = s.TokenText()
	if tok == scanner.EOF{
		panic(ErrSyntax)
	}

	return col, op, val
}

func (qs *QuerySelect) parseWhere(s *scanner.Scanner) {
	qs.Where = (*where)(parseWhere(s))
}

func parseWhere(s *scanner.Scanner) (*statement.WhereStatement) {
	var logOp string
	sttmnts := []*statement.WhereStatement{}

	for {
		tok := s.Scan()
		word := s.TokenText()
		_, isKW := keyWords[word]
		if tok == scanner.EOF {
			panic(ErrSyntax)
		} else if word == "(" {
			sttmnts = append(sttmnts, parseWhere(s))
		} else if word == ")" || word == ";" || isKW {
			break
		} else if _, ok := logicalOperators[word]; ok {
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
