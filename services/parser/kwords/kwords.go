package kwords

import "go-dbms/pkg/types"

var KeyWords = map[string]struct{}{
	"SELECT":      {},
	"FROM":        {},
	"WHERE_INDEX": {},
	"WHERE":       {},
	"GROUP_BY":    {},
	"HAVING":      {},
	"ORDER_BY":    {},
	"LIMIT":       {},

	"INSERT": {},
	"VALUES": {},

	"DELETE": {},

	"UPDATE": {},
	"SET":    {},

	"PREPARE": {},
	"TABLE":   {},
	"ROWS":    {},
}

var IndexOperators = map[types.Operator]struct{}{
	">":  {},
	">=": {},
	"=":  {},
	"<":  {},
	"<=": {},
}

var LogicalOperators = map[string]struct{}{
	"AND": {},
	"OR":  {},
}
