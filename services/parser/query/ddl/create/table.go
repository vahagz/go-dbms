package create

import (
	"text/scanner"

	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query/ddl/create/types"
)

/*
CREATE TABLE <tableName> (
	<columnName> <type> [AUTO INCREMENT],
	...
)
PRIMARY KEY (<...columns>) <primaryKeyName>
[, INDEX(<...columns>) <indexName>]
...;
*/
type QueryCreateTable struct {
	*QueryCreate
	Database string                   `json:"database"`
	Name     string                   `json:"name"`
	Columns  []*column.Column         `json:"columns"`
	Indexes  []*QueryCreateTableIndex `json:"indexes"`
}

func (qct *QueryCreateTable) Parse(s *scanner.Scanner) (err error) {
	defer func ()  {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	qct.Target = TABLE

	qct.parseName(s)
	qct.parseColumns(s)
	
	qct.Indexes = []*QueryCreateTableIndex{}

	qct.parsePrimaryKey(s)
	qct.parseIndexes(s)

	return nil
}

func (qct *QueryCreateTable) parseName(s *scanner.Scanner) {
	s.Scan()
	qct.Name = s.TokenText()
}

func (qct *QueryCreateTable) parseColumns(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "(" {
		panic(errors.ErrSyntax)
	}

	qct.Columns = []*column.Column{}
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		if word == ")" {
			break
		}
		qct.parseColumn(s)
	}

	s.Scan()
}

func (qct *QueryCreateTable) parseColumn(s *scanner.Scanner) {
	scope := 0
	col := &column.Column{}
	colName := s.TokenText()

	if _, isKW := kwords.KeyWords[colName]; isKW {
		panic(errors.ErrSyntax)
	}
	col.Name = colName

	tokens := []string{}
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		if word == "(" {
			scope++
		}
		if scope == 0 && (word == "," || word == ")") {
			break
		}
		if word == ")" {
			scope--
		}

		tokens = append(tokens, word)
	}

	col.Meta = types.Parse(tokens)
	col.Typ = col.Meta.GetCode()

	qct.Columns = append(qct.Columns, col)
}

func (qct *QueryCreateTable) parsePrimaryKey(s *scanner.Scanner) {
	if s.TokenText() != "PRIMARY" {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	if s.TokenText() != "KEY" {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	if s.TokenText() != "(" {
		panic(errors.ErrSyntax)
	}

	pk := &QueryCreateTableIndex{
		IndexOptions: &index.IndexOptions{
			Columns: []string{},
			Primary: true,
			Uniq:    true,
		},
	}
	qct.Indexes = append(qct.Indexes, pk)

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		word := s.TokenText()
		if _, isKW := kwords.KeyWords[word]; isKW {
			panic(errors.ErrSyntax)
		}

		pk.Columns = append(pk.Columns, word)

		s.Scan()
		word = s.TokenText()
		if word == ")" {
			break
		} else if word != "," {
			panic(errors.ErrSyntax)
		}
	}

	s.Scan()
	pk.Name = s.TokenText()
	if _, isKW := kwords.KeyWords[pk.Name]; isKW {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	word := s.TokenText()
	if word != "," && word != ";" {
		panic(errors.ErrSyntax)
	}
}

func (qct *QueryCreateTable) parseIndexes(s *scanner.Scanner) {
	word := s.TokenText()
	if word == ";" {
		return
	} else if word != "," {
		panic(errors.ErrSyntax)
	}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		if s.TokenText() != "INDEX" {
			panic(errors.ErrSyntax)
		}

		s.Scan()
		if s.TokenText() != "(" {
			panic(errors.ErrSyntax)
		}

		idx := &QueryCreateTableIndex{
			IndexOptions: &index.IndexOptions{
				Columns: []string{},
			},
		}
		qct.Indexes = append(qct.Indexes, idx)
	
		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			word := s.TokenText()
			if _, isKW := kwords.KeyWords[word]; isKW {
				panic(errors.ErrSyntax)
			}
	
			idx.Columns = append(idx.Columns, word)
	
			s.Scan()
			word = s.TokenText()
			if word == ")" {
				break
			} else if word != "," {
				panic(errors.ErrSyntax)
			}
		}
	
		s.Scan()
		idx.Name = s.TokenText()
		if _, isKW := kwords.KeyWords[idx.Name]; isKW {
			panic(errors.ErrSyntax)
		}
	
		s.Scan()
		word := s.TokenText()
		if word == "UNIQUE" {
			idx.Uniq = true
			s.Scan()
			word = s.TokenText()
		}

		if word != "," && word != ";" {
			panic(errors.ErrSyntax)
		}
	}
}
