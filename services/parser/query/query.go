package query

import "text/scanner"

type QueryType string

const (
	INSERT   QueryType = "INSERT_INTO"
	SELECT   QueryType = "SELECT"
	UPDATE   QueryType = "UPDATE"
	DELETE   QueryType = "DELETE_FROM"
	CREATE   QueryType = "CREATE"
	ALTER    QueryType = "ALTER"
	DROP     QueryType = "DROP"
	TRUNCATE QueryType = "TRUNCATE"
	RENAME   QueryType = "RENAME"
)

type Querier interface {
	GetType() QueryType
	Parse(s *scanner.Scanner) error
}

type Query struct {
	Type QueryType `json:"type"`
}

func (q *Query) GetType() QueryType {
	return q.Type
}
