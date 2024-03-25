package query

import "text/scanner"

type QueryType string

const (
	INSERT   QueryType = "INSERT"
	SELECT   QueryType = "SELECT"
	UPDATE   QueryType = "UPDATE"
	DELETE   QueryType = "DELETE"
	CREATE   QueryType = "CREATE"
	ALTER    QueryType = "ALTER"
	DROP     QueryType = "DROP"
	TRUNCATE QueryType = "TRUNCATE"
	RENAME   QueryType = "RENAME"
	PREPARE  QueryType = "PREPARE"
)

type Querier interface {
	GetType() QueryType
}

type QueryParser interface {
	Querier
	Parse(s *scanner.Scanner) error
}

type Query struct {
	Type QueryType `json:"type"`
}

func (q *Query) GetType() QueryType {
	return q.Type
}
