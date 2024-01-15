package create

import "go-dbms/services/parser/query"

type QueryCreateTarget string

const (
	DATABASE QueryCreateTarget = "DATABASE"
	TABLE    QueryCreateTarget = "TABLE"
	INDEX    QueryCreateTarget = "INDEX"
)

type QueryCreate struct {
	query.Query
	Target QueryCreateTarget `json:"target"`
}
