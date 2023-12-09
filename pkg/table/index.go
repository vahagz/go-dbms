package table

import (
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
)

type index struct {
	tree    *bptree.BPlusTree
	columns []string
	uniq    bool
}

type operator struct {
	cmpOption  map[int]bool
	scanOption bptree.ScanOptions
}

var operatorMapping = map[string]operator {
	"<":  {
		cmpOption:  map[int]bool{ 1: true },
		scanOption: bptree.ScanOptions{Reverse: true, Strict: false},
	},
	"<=": {
		cmpOption:  map[int]bool{ 1: true, 0: true },
		scanOption: bptree.ScanOptions{Reverse: true, Strict: true},
	},
	"=":  {
		cmpOption:  map[int]bool{ 0:  true },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: true},
	},
	">=": {
		cmpOption:  map[int]bool{ 0:  true, -1: true },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: true},
	},
	">":  {
		cmpOption:  map[int]bool{ -1:  true },
		scanOption: bptree.ScanOptions{Reverse: false, Strict: false},
	},
}

func (i *index) Insert(ptr *data.RecordPointer, values map[string]types.DataType) error {
	key, err := i.key(values)
	if err != nil {
		return err
	}

	val, err := ptr.MarshalBinary()
	if err != nil {
		return err
	}

	return i.tree.Put(key, val, &bptree.PutOptions{
		// Uniq: i.uniq,
		Update: false,
	})
}

func (i *index) Find(
	values map[string]types.DataType,
	operator string,
	scanFn func(ptr *data.RecordPointer) (map[string]types.DataType, error),
) ([]map[string]types.DataType, error) {
	key, err := i.key(values)
	if err != nil {
		return nil, err
	}

	op := operatorMapping[operator]
	result := []map[string]types.DataType{}
	err = i.tree.Scan(key, op.scanOption, func(k [][]byte, v []byte) (bool, error) {
		if i.stop(k, operator, key) {
			return true, nil
		}

		ptr := &data.RecordPointer{}
		err := ptr.UnmarshalBinary(v)
		if err != nil {
			return false, err
		}

		row, err := scanFn(ptr)
		if err != nil {
			return false, err
		}

		result = append(result, row)
		return false, nil
	})

	return result, err
}

func (i *index) Close() error {
	return i.tree.Close()
}

func (i *index) key(values map[string]types.DataType) ([][]byte, error) {
	key := make([][]byte, len(i.columns))

	for i, col := range i.columns {
		if col, ok := values[col]; ok {
			key[i] = col.Bytes()
		} else {
			key[i] = nil
		}
	}

	return key, nil
}

func (i *index) stop(
	currentKey [][]byte,
	operator string,
	searchingKey [][]byte,
) bool {
	cmp := helpers.CompareMatrix(searchingKey, currentKey)
	_, ok := operatorMapping[operator].cmpOption[cmp]
	return !ok
}
