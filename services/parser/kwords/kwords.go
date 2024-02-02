package kwords

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
}

var IndexOperators = map[string]struct{}{
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
