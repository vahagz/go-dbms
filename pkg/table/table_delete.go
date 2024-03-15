package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Delete(filter *statement.WhereStatement) stream.Reader[types.DataRow] {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	s := stream.New[types.DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(t.delete(t.Find(filter).Slice(), t.Indexes, func(row types.DataRow) error {
			s.Push(row)
			return nil
		}))
	}()
	return s
}

func (t *Table) DeleteByIndex(
	name string,
	start, end *index.Filter,
	filter *statement.WhereStatement,
) (stream.Reader[types.DataRow], error) {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	delIndex, ok := t.Indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	s := stream.New[types.DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(t.delete(
			delIndex.ScanEntries(start, end, filter),
			t.Indexes,
			func(row types.DataRow) error {
				s.Push(row)
				return nil
			},
		))
	}()
	return s, nil
}

func (t *Table) delete(
	entries []index.Entry,
	indexesToUpdate map[string]*index.Index,
	scanFn func(row types.DataRow) error,
) error {
	for _, e := range entries {
		t.deleteRow(e.Ptr, e.Row, indexesToUpdate)
		if err := scanFn(t.row2pk(e.Row)); err != nil {
			return errors.Wrap(err, "failed to delete row")
		}
	}

	return nil
}

func (t *Table) deleteRow(
	ptr allocator.Pointable,
	row types.DataRow,
	indexesToUpdate map[string]*index.Index,
) {
	for _, i := range indexesToUpdate {
		t.deleteIndex(i, row)
	}
	t.DF.DeleteMem(ptr)
}

func (t *Table) deleteIndex(i *index.Index, row types.DataRow) {
	if _, err := i.Delete(row, !t.isPK(i)); err != nil {
		panic(errors.Wrapf(err, "error while deleting from index '%s'", i.Meta().Name))
	}
}
