package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Insert(in stream.Reader[DataRow], out stream.Writer[DataRow]) error {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	defer out.Close()
	for row, ok := in.Pop(); ok; row, ok = in.Pop() {
		t.setDefaults(row)
		if err := t.validateMap(row); err != nil {
			return errors.Wrap(err, "validation error")
		} else if err := t.canInsert(row); err != nil {
			return errors.Wrapf(err, "can't insert row")
		}
		out.Push(t.insert(row))
	}

	return nil
}

func (t *Table) insert(row DataRow) DataRow {
	ptr, err := t.DF.InsertMem(t.map2row(row))
	if err != nil {
		panic(errors.Wrap(err, "failed to insert into datafile"))
	}

	var pk DataRow
	for _, index := range t.Indexes {
		t.insertIndex(index, ptr, row)
		if t.isPK(index) {
			pkCols := index.Columns()
			pk = make(DataRow, len(pkCols))
			for _, c := range pkCols {
				pk[c.Name] = row[c.Name]
			}
		}
	}

	return pk
}

func (t *Table) insertIndex(i *index.Index, ptr allocator.Pointable, row DataRow) {
	if err := i.Insert(ptr, row); err != nil {
		panic(errors.Wrapf(err, "failed to insert into index:'%s'", i.Meta().Name))
	}
}

func (t *Table) canInsert(row DataRow) error {
	canInsert := true
	var conflictIndex string
	for _, i := range t.Indexes {
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

func (t *Table) canInsertIndex(i *index.Index, row DataRow) bool {
	return i.CanInsert(row)
}
