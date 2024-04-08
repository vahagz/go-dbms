package statement

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/projection"
)

type Statement struct {
	Left  *projection.Projection
	Op    types.Operator
	Right *projection.Projection
}
