package index

import (
	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"

	"github.com/vahagz/bptree"
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

func (i *Index) PrepareSpace(rows int) {
	i.tree.PrepareSpace(uint32(rows * int(i.tree.HeapSize() / i.tree.Count())))
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
