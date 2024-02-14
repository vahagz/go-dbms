package statement

import "go-dbms/services/parser/query/dml/projection"

type Statement struct {
	Left  *projection.Projection
	Op    string
	Right *projection.Projection
}
