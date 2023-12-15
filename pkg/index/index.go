package index

import (
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
)

type Index struct {
	df      *data.DataFile
	tree    *bptree.BPlusTree
	columns []string
	uniq    bool
}

func New(df *data.DataFile, tree *bptree.BPlusTree, columns []string, uniq bool) *Index {
	return &Index{
		df:      df,
		tree:    tree,
		columns: columns,
		uniq:    uniq,
	}
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

func (i *Index) Insert(ptr allocator.Pointable, values map[string]types.DataType) error {
	key, err := i.key(values)
	if err != nil {
		return err
	}

	val, err := ptr.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = i.tree.PutMem(key, val, bptree.PutOptions{Update: false})
	return err
}

func (i *Index) Delete(values map[string]types.DataType) error {
	key, err := i.key(values)
	if err != nil {
		return err
	}

	i.tree.DelMem(key)
	return nil
}

func (i *Index) Find(
	values map[string]types.DataType,
	operator string,
	scanFn func(ptr allocator.Pointable) (map[string]types.DataType, error),
) ([]map[string]types.DataType, error) {
	key, err := i.key(values)
	if err != nil {
		return nil, err
	}

	op := operatorMapping[operator]
	opts := op.scanOption
	opts.Key = key
	result := []map[string]types.DataType{}
	err = i.tree.Scan(op.scanOption, func(k [][]byte, v []byte) (bool, error) {
		if i.stop(k, operator, key) {
			return true, nil
		}

		ptr := i.df.Pointer()
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

func (i *Index) Scan(
	opts ScanOptions,
	scanFn func(key [][]byte, ptr allocator.Pointable) (bool, error),
) error {
	return i.tree.Scan(opts.ScanOptions, func(key [][]byte, val []byte) (bool, error) {
		ptr := i.df.Pointer()
		ptr.UnmarshalBinary(val)
		return scanFn(key, ptr)
	})
}

func (i *Index) Columns() []string {
	cp := make([]string, len(i.columns))
	copy(cp, i.columns)
	return cp
}

func (i *Index) KeySize() int {
	return i.tree.Options().MaxKeySize
}

func (i *Index) Close() error {
	return i.tree.Close()
}

func (i *Index) key(values map[string]types.DataType) ([][]byte, error) {
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

func (i *Index) stop(
	currentKey [][]byte,
	operator string,
	searchingKey [][]byte,
) bool {
	cmp := helpers.CompareMatrix(searchingKey, currentKey)
	_, ok := operatorMapping[operator].cmpOption[cmp]
	return !ok
}
