package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Insert(values map[string]types.DataType) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.setDefaults(values)

	if err := t.validateMap(values); err != nil {
		return errors.Wrap(err, "validation error")
	}

	if err := t.canInsert(values); err != nil {
		return errors.Wrapf(err, "can't insert row")
	}

	t.insert(values)

	return nil
}

func (t *Table) insert(row map[string]types.DataType) {
	ptr, err := t.df.Insert(t.map2row(row))
	if err != nil {
		panic(errors.Wrap(err, "failed to insert into datafile"))
	}

	for _, index := range t.indexes {
		t.insertIndex(index, ptr, row)
	}
}

func (t *Table) insertIndex(i *index.Index, ptr allocator.Pointable, row map[string]types.DataType) {
	if err := i.Insert(ptr, row); err != nil {
		panic(errors.Wrapf(err, "failed to insert into index:'%s'", i.Meta().Name))
	}
}

func (t *Table) canInsert(row map[string]types.DataType) error {
	canInsert := true
	var conflictIndex string
	for _, i := range t.indexes {
		if !t.canInsertIndex(i, row) {
			canInsert = false
			conflictIndex = i.Meta().Name
			break
		}
	}
	
	if !canInsert {
		return fmt.Errorf("can't insert, '%s' causes conflict", conflictIndex)
	}
	return nil
}

func (t *Table) canInsertIndex(i *index.Index, row map[string]types.DataType) bool {
	return i.CanInsert(row)
}
