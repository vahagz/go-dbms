package index

import (
	"fmt"
	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"

	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

type Index struct {
	meta    *Meta
	df      *data.DataFile
	tree    *bptree.BPlusTree
	columns []*column.Column
	uniq    bool
	primary *Index
}

func New(
	meta *Meta,
	df *data.DataFile,
	tree *bptree.BPlusTree,
	columns []*column.Column,
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

func (i *Index) ScanFilter(start, end *Filter, scanFn func(ptr allocator.Pointable) (stop bool, err error)) error {
	op := operatorMapping[start.Operator]
	opts := op.scanOption
	prefixColsCount := len(start.Value)
	postfixColsCount := 0

	for _, col := range i.columns {
		if _, ok := start.Value[col.Name]; !ok {
			postfixColsCount++
			if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
				start.Value[col.Name] = types.Type(col.Meta).Fill()
			} else {
				start.Value[col.Name] = types.Type(col.Meta).Zero()
			}
		}
	}

	endKey := i.key(end.Value)
	opts.Key = i.key(start.Value)
	searchingKey := opts.Key
	if postfixColsCount > 0 {
		searchingKey = i.removeAutoSetCols(searchingKey, prefixColsCount, postfixColsCount)
	}

	return i.tree.Scan(opts, func(k [][]byte, v []byte) (bool, error) {
		if !i.tree.IsUniq() {
			k = i.tree.RemoveSuffix(k)
		}
		if postfixColsCount > 0 {
			k = i.removeAutoSetCols(k, prefixColsCount, postfixColsCount)
		}
		fmt.Println(
			shouldStop(k, start.Operator, searchingKey),
			shouldStop(k, end.Operator, endKey),
			string(searchingKey[0]), string(searchingKey[1]),
			string(endKey[0]), string(endKey[1]),
			string(k[0]), string(k[1]),
		)
		if shouldStop(k, start.Operator, searchingKey) || shouldStop(k, end.Operator, endKey) {
			return true, nil
		}

		ptr := i.df.Pointer()
		if err := ptr.UnmarshalBinary(v); err != nil {
			return false, err
		}
		return scanFn(ptr)
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

func (i *Index) Columns() []*column.Column {
	return i.columns
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
		if col, ok := values[col.Name]; ok {
			key[i] = col.Bytes()
		} else {
			key[i] = nil
		}
	}

	return key
}

func (i *Index) removeAutoSetCols(k [][]byte, prefixCount, postfixCount int) [][]byte {
	newKey := make([][]byte, 0, len(k) - postfixCount)
	newKey = append(newKey, k[:prefixCount]...)
	if len(k) >= prefixCount + postfixCount {
		newKey = append(newKey, k[prefixCount + postfixCount:]...)
	}
	return newKey
}
