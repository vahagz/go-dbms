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
	DB         string
	Table      string
	UseIndex   string
	Where      *statement.WhereStatement
	WhereIndex *WhereIndex
}

func (qd *QueryDelete) Parse(s *scanner.Scanner, ps query.Parser) (err error) {
	defer helpers.RecoverOnError(&err)()

	qd.Type = query.DELETE

	qd.parseFrom(s)
	qd.parseUseIndex(s)
	qd.parseWhereIndex(s, ps)
	qd.parseWhere(s, ps)

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

func (qs *QueryDelete) parseUseIndex(s *scanner.Scanner) {
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

func (qs *QueryDelete) parseWhereIndex(s *scanner.Scanner, ps query.Parser) {
	qs.WhereIndex = parseWhereIndex(s, ps)
}

func (qd *QueryDelete) parseWhere(s *scanner.Scanner, ps query.Parser) {
	word := s.TokenText()
	if word != "WHERE" {
		return
	}

	qd.Where = parseWhere(s, ps)
}
