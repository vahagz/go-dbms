package statement

import "go-dbms/pkg/types"

type Statement struct {
	column   string
	operator string
	value    types.DataType
}

func (s *Statement) Column() string {
	return s.column
}

func (s *Statement) Operator() string {
	return s.operator
}

func (s *Statement) Value() types.DataType {
	return s.value
}
