package dml

var keyWords = map[string]struct{}{
	"SELECT":      {},
	"FROM":        {},
	"WHERE_INDEX": {},
	"WHERE":       {},
	"GROUP_BY":    {},
	"HAVING":      {},
	"ORDER_BY":    {},
	"LIMIT":       {},

	"INSERT_INTO": {},
	"VALUES":      {},

	"DELETE_FROM": {},
}

var indexOperators = map[string]struct{}{
	">":  {},
	">=": {},
	"=":  {},
	"<":  {},
	"<=": {},
}

var logicalOperators = map[string]struct{}{
	"AND": {},
	"OR":  {},
}
