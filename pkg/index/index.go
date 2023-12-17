package index

import (
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
)

type Index struct {
	meta    *Meta
	df      *data.DataFile
	tree    *bptree.BPlusTree
	columns []string
	uniq    bool
	primary *Index
}

func New(
	meta *Meta,
	df *data.DataFile,
	tree *bptree.BPlusTree,
	columns []string,
	uniq bool,
) *Index {
	return &Index{
		meta:    meta,
		df:      df,
		tree:    tree,
		columns: columns,
		uniq:    uniq,
	}
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

func (i *Index) SetPK(pk *Index) {
	i.primary = pk
}

func (i *Index) Insert(dataPtr allocator.Pointable, values map[string]types.DataType) error {
	val, err := dataPtr.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = i.tree.PutMem(
		i.key(values),
		i.primary.key(values),
		val,
		bptree.PutOptions{Update: false},
	)
	return err
}

func (i *Index) Delete(values map[string]types.DataType, withPK bool) (int, error) {
	var pk [][]byte
	if withPK {
		pk = i.primary.key(values)
	}

	return i.tree.DelMem(i.key(values), pk)
}

func (i *Index) Find(
	values map[string]types.DataType,
	withPK bool,
	operator string,
	scanFn func(ptr allocator.Pointable) error,
) error {
	var pk [][]byte
	if withPK {
		pk = i.primary.key(values)
	}

	op := operatorMapping[operator]
	opts := op.scanOption
	opts.Key = append(i.key(values), pk...)
	return i.tree.Scan(opts, func(k [][]byte, v []byte) (bool, error) {
		if i.stop(k, operator, opts.Key) {
			return true, nil
		}

		ptr := i.df.Pointer()
		err := ptr.UnmarshalBinary(v)
		if err != nil {
			return false, err
		}
		return false, scanFn(ptr)
	})
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

func (i *Index) Meta() *Meta {
	return i.meta
}

func (i *Index) CanInsert(values map[string]types.DataType) bool {
	return i.tree.CanInsert(i.key(values), i.primary.key(values))
}

func (i *Index) Columns() []string {
	cp := make([]string, len(i.columns))
	copy(cp, i.columns)
	return cp
}

func (i *Index) Options() bptree.Options {
	return i.tree.Options()
}

func (i *Index) Close() error {
	return i.tree.Close()
}

func (i *Index) Remove() {
	i.tree.Remove()
}

func (i *Index) key(values map[string]types.DataType) [][]byte {
	if i == nil {
		return nil
	}

	key := make([][]byte, len(i.columns))

	for i, col := range i.columns {
		if col, ok := values[col]; ok {
			key[i] = col.Bytes()
		} else {
			key[i] = nil
		}
	}

	return key
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
