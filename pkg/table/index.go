package table

import (
	"bytes"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/data"
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

	return i.tree.Put(key, val, &bptree.PutOptions{
		Uniq: i.uniq,
		Update: false,
	})
}

func (i *index) Find(values map[string]types.DataType, reverse bool) ([]*data.RecordPointer, error) {
	key, err := i.key(i.tuple(values))
	if err != nil {
		return nil, err
	}

	result := []*data.RecordPointer{}
	err = i.tree.Scan(key, reverse, func(k, v []byte) (bool, error) {
		if !bytes.Equal(key, k) {
			return true, nil
		}

		ptr := &data.RecordPointer{}
		err := ptr.UnmarshalBinary(v)
		if err != nil {
			return false, err
		}
		result = append(result, ptr)
		return false, nil
	})

	return result, err
}

func (i *index) Close() error {
	return i.tree.Close()
}

func (i *index) key(tuple []types.DataType) ([]byte, error) {
	key := []byte{}
	underscore := false

	for _, col := range tuple {
		colBytes, err := col.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if underscore {
			key = append(key, '_')
		}
		underscore = true
		key = append(key, colBytes...)
	}

	return key, nil
}

func (i *index) tuple(values map[string]types.DataType) []types.DataType {
	tuple := []types.DataType{}
	for _, columnName := range i.columns {
		data, ok := values[columnName]
		if ok {
			tuple = append(tuple, data)
		}
	}
	return tuple
}
