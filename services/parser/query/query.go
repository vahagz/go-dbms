package query

type QueryCommandType string

const (
	INSERT   QueryCommandType = "INSERT"
	SELECT   QueryCommandType = "SELECT"
	UPDATE   QueryCommandType = "UPDATE"
	DELETE   QueryCommandType = "DELETE"
	CREATE   QueryCommandType = "CREATE"
	ALTER    QueryCommandType = "ALTER"
	DROP     QueryCommandType = "DROP"
	TRUNCATE QueryCommandType = "TRUNCATE"
	RENAME   QueryCommandType = "RENAME"
)

type Querier interface {
	Type() QueryCommandType
}

type Query struct {
	Command QueryCommandType `json:"command"`
}

func (q *Query) Type() QueryCommandType {
	return q.Command
}
