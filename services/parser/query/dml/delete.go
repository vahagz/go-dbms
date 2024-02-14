package dml

import (
	"text/scanner"

	"go-dbms/pkg/statement"
	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
	"go-dbms/util/helpers"
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
	WhereIndex *WhereIndex               `json:"where_index"`
}

func (qd *QueryDelete) Parse(s *scanner.Scanner) (err error) {
	defer helpers.RecoverOnError(&err)()

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

func (qs *QueryDelete) parseWhereIndex(s *scanner.Scanner) {
	qs.WhereIndex = parseWhereIndex(s)
}

func (qd *QueryDelete) parseWhere(s *scanner.Scanner) {
	word := s.TokenText()
	if word != "WHERE" {
		return
	}

	qd.Where = parseWhere(s)
}
