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

var operatorMapping = map[string]struct {
	cmpOption map[int]bool
	reverse   bool
	strict    bool
} {
	"<":  {
		cmpOption: map[int]bool{ 1: true },
		reverse:   true,
		strict:    false,
	},
	"<=": {
		cmpOption: map[int]bool{ 1: true, 0: true },
		reverse:   true,
		strict:    true,
	},
	"=":  {
		cmpOption: map[int]bool{ 0:  true },
		strict:    true,
	},
	">=": {
		cmpOption: map[int]bool{ 0:  true, -1: true },
		reverse:   false,
		strict:    true,
	},
	">":  {
		cmpOption: map[int]bool{ -1:  true },
		reverse:   false,
		strict:    false,
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
		Uniq: i.uniq,
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
	reverse, strict := op.reverse, op.strict
	result := []map[string]types.DataType{}
	err = i.tree.Scan(key, reverse, strict, func(k [][]byte, v []byte) (bool, error) {
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
