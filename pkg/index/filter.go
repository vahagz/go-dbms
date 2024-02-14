package index

import (
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"

	"github.com/vahagz/bptree"
)

type Filter struct {
	Operator string
	Left, Right *projection.Projection
}

type operator struct {
	cmpOption  map[int]struct{}
	scanOption bptree.ScanOptions
}

var operatorMapping = map[string]operator {
	"<":  {
		cmpOption:  map[int]struct{}{ 1: {} },
		scanOption: bptree.ScanOptions{Reverse: true, Strict: false},
	},
	"<=": {
		cmpOption:  map[int]struct{}{ 1: {}, 0: {} },
		scanOption: bptree.ScanOptions{Reverse: true, Strict: true},
	},
	"=":  {
		cmpOption:  map[int]struct{}{ 0: {} },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: true},
	},
	">=": {
		cmpOption:  map[int]struct{}{ 0: {}, -1: {} },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: true},
	},
	">":  {
		cmpOption:  map[int]struct{}{ -1: {} },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: false},
	},
}

func shouldStop(
	currentKey [][]byte,
	operator string,
	searchingKey [][]byte,
) bool {
	cmp := helpers.CompareMatrix(searchingKey, currentKey)
	_, ok := operatorMapping[operator].cmpOption[cmp]
	return !ok
}
