package table

import (
	"go-dbms/pkg/bptree"
	data "go-dbms/pkg/slotted_data"
	"go-dbms/pkg/types"
)

type index struct {
	tree    *bptree.BPlusTree
	columns []string
	uniq    bool
}

func (i *index) Insert(ptr *data.RecordPointer, values map[string]types.DataType) error {
	tuple := i.tuple(values)

	key, err := i.key(tuple)
	if err != nil {
		return err
	}

	val, err := ptr.MarshalBinary()
	if err != nil {
		return err
	}

	return i.tree.Put(key, val)
}

func (i *index) FindOne(values map[string]types.DataType) (*data.RecordPointer, error) {
	key, err := i.key(i.tuple(values))
	if err != nil {
		return nil, err
	}

	ptrBytes, err := i.tree.Get(key)
	if err != nil {
		return nil, err
	}

	ptr := &data.RecordPointer{}
	err = ptr.UnmarshalBinary(ptrBytes)
	return ptr, err
}

func (i *index) Close() error {
	return i.tree.Close()
}

func (i *index) key(tuple []types.DataType) ([]byte, error) {
	key := []byte{}

	for _, col := range tuple {
		colBytes, err := col.MarshalBinary()
		if err != nil {
			return nil, err
		}
		key = append(key, colBytes...)
	}

	return key, nil
}

func (i *index) tuple(values map[string]types.DataType) []types.DataType {
	tuple := []types.DataType{}
	for _, columnName := range i.columns {
		tuple = append(tuple, values[columnName])
	}
	return tuple
}
