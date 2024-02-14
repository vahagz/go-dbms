package dml

import (
	"strconv"
	"text/scanner"

	"go-dbms/services/parser/errors"
	"go-dbms/services/parser/kwords"
	"go-dbms/services/parser/query"
	"go-dbms/util/helpers"
)

/*
PREPARE TABLE <tableName> ROWS <n>;
*/
type QueryPrepare struct {
	query.Query
	DB    string
	Table string
	Rows  int
}


func (qp *QueryPrepare) Parse(s *scanner.Scanner) (err error) {
	defer helpers.RecoverOnError(&err)()

	qp.Type = query.PREPARE

	qp.parseTable(s)
	qp.parseRows(s)

	return nil
}

func (qp *QueryPrepare) parseTable(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "TABLE" {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	qp.Table = s.TokenText()
	if _, isKW := kwords.KeyWords[qp.Table]; isKW {
		panic(errors.ErrSyntax)
	}
}

func (qp *QueryPrepare) parseRows(s *scanner.Scanner) {
	s.Scan()
	if s.TokenText() != "ROWS" {
		panic(errors.ErrSyntax)
	}

	s.Scan()
	rows, err := strconv.Atoi(s.TokenText())
	if err != nil {
		panic(err)
	}
	
	qp.Rows = rows

	s.Scan()
	if s.TokenText() != ";" {
		panic(errors.ErrSyntax)
	}
}
