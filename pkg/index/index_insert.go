package index

import (
	"go-dbms/pkg/types"

	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (i *Index) Insert(dataPtr allocator.Pointable, values types.DataRow) error {
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
