package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/types"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
	"golang.org/x/sync/errgroup"
)

func (t *Table) Insert(in stream.Reader[types.DataRow]) (stream.Reader[types.DataRow], *errgroup.Group) {
	eg := &errgroup.Group{}
	out := stream.New[types.DataRow](0)

	eg.Go(func () error {
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
	})

	return out, eg
}

func (t *Table) insert(row types.DataRow) types.DataRow {
	ptr, err := t.DF.InsertMem(t.map2row(row))
	if err != nil {
		panic(errors.Wrap(err, "failed to insert into datafile"))
	}

	var pk types.DataRow
	for _, index := range t.Indexes {
		t.insertIndex(index, ptr, row)
		if t.isPK(index) {
			pkCols := index.Columns()
			pk = make(types.DataRow, len(pkCols))
			for _, c := range pkCols {
				pk[c.Name] = row[c.Name]
			}
		}
	}

	return pk
}

func (t *Table) insertIndex(i *index.Index, ptr allocator.Pointable, row types.DataRow) {
	if err := i.Insert(ptr, row); err != nil {
		panic(errors.Wrapf(err, "failed to insert into index:'%s'", i.Meta().Name))
	}
}

func (t *Table) canInsert(row types.DataRow) error {
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

func (t *Table) canInsertIndex(i *index.Index, row types.DataRow) bool {
	return i.CanInsert(row)
}
