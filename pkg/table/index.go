package table

import (
	"bytes"
	"fmt"
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

type operatorConfig struct {
	cmpOption map[int]bool
	reverse   bool
	strict    bool
}
var operatorMapping = map[string]operatorConfig {
	"<":  {
		cmpOption: map[int]bool{ -1: true },
		reverse:   false,
		strict:    false,
	},
	"<=": {
		cmpOption: map[int]bool{ -1: true, 0: true },
		reverse:   false,
		strict:    true,
	},
	"=":  {
		cmpOption: map[int]bool{ 0:  true },
		strict:    true,
	},
	">=": {
		cmpOption: map[int]bool{ 0:  true, 1: true },
		reverse:   true,
		strict:    true,
	},
	">":  {
		cmpOption: map[int]bool{ 1:  true },
		reverse:   true,
		strict:    false,
	},
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

func (i *index) Find(
	values map[string]types.DataType,
	operator string,
	scanFn func(ptr *data.RecordPointer) (map[string]types.DataType, error),
) ([]map[string]types.DataType, error) {
	tuple := i.tuple(values)
	key, err := i.key(tuple)
	if err != nil {
		return nil, err
	}

	op := operatorMapping[operator]
	reverse, strict := op.reverse, op.strict
	result := []map[string]types.DataType{}
	err = i.tree.Scan(key, reverse, strict, func(k, v []byte) (bool, error) {
		stop, skip := i.where(key, operator, k)
		if stop {
			return true, nil
		} else if skip {
			return false, nil
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

		if operator == "=" {
			for _, column := range i.columns {
				if rowCol, ok := row[column]; ok {
					rowCol, err := rowCol.MarshalBinary()
					if err != nil {
						return false, err
					}

					if valCol, ok := values[column]; ok {
						val, err := valCol.MarshalBinary()
						if err != nil {
							return false, err
						}
	
						if !bytes.Equal(rowCol, val) {
							return true, nil
						}
					}
				}
			}
		}

		result = append(result, row)
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

func (i *index) where(
	searchingKey []byte,
	operator string,
	currentKey []byte,
) (stop bool, skip bool) {
	min := helpers.Min(len(searchingKey), len(currentKey))
	fmt.Println(string(searchingKey), operator, string(currentKey), bytes.Compare(searchingKey[:min], currentKey[:min]))
	if operator == "=" {
		_, ok := operatorMapping[operator].cmpOption[bytes.Compare(searchingKey[:min], currentKey[:min])]
		return !ok, false
	}

	cmp := bytes.Compare(searchingKey[:min], currentKey[:min])
	if cmp == 0 && (operator == ">" || operator == "<") {
		return false, true
	}

	_, ok := operatorMapping[operator].cmpOption[cmp]
	return !ok, false
}
