package table

import (
	"fmt"
	"slices"

	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Update(filter *statement.WhereStatement, updateValuesMap DataRow) stream.Reader[DataRow] {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	s := stream.New[DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(t.update(t.Find(filter).Slice(), updateValuesMap, t.Indexes, func(row DataRow) error {
			s.Push(row)
			return nil
		}))
	}()
	return s
}

func (t *Table) UpdateByIndex(
	name string,
	start, end *index.Filter,
	filter *statement.WhereStatement,
	updateValuesMap DataRow,
) (stream.Reader[DataRow], error) {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	updIndex, ok := t.Indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	s := stream.New[DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(t.update(
			updIndex.ScanEntries(start, end, filter),
			updateValuesMap,
			t.getAffectedIndexes(updIndex, updateValuesMap),
			func(row DataRow) error {
				s.Push(row)
				return nil
			},
		))
	}()
	return s, nil
}

func (t *Table) update(
	entries []index.Entry,
	updateValuesMap DataRow,
	indexesToUpdate map[string]*index.Index,
	scanFn func(row DataRow) error,
) error {
	for _, e := range entries {
		updated := make(DataRow, len(e.Row))
		for col, oldVal := range e.Row {
			if newVal, ok := updateValuesMap[col]; ok {
				updated[col] = newVal
			} else {
				updated[col] = oldVal
			}
		}

		if err := t.updateRow(e.Ptr, e.Row, updated, indexesToUpdate); err != nil {
			return errors.Wrap(err, "failed to update table")
		}

		if err := scanFn(t.row2pk(e.Row)); err != nil {
			return errors.Wrap(err, "failed to update table")
		}
	}

	return nil
}

func (t *Table) updateRow(
	oldPtr allocator.Pointable,
	oldRow, newRow DataRow,
	indexesToUpdate map[string]*index.Index,
) error {
	newPtr := t.DF.UpdateMem(oldPtr, t.map2row(newRow))
	ptrUpdated := !oldPtr.Equal(newPtr) // pointer in datafile updated
	updatedIndexes := make([]*index.Index, 0, len(t.Indexes))
	var updateErr error

	for name, i := range t.Indexes {
		_, indexShouldUpdate := indexesToUpdate[name]
		if ptrUpdated || indexShouldUpdate {
			if updateErr = t.updateIndex(i, newPtr, oldRow, newRow); updateErr != nil {
				break
			}
			updatedIndexes = append(updatedIndexes, i)
		}
	}

	// rollback if error occurred
	if updateErr != nil {
		newPtr = t.DF.UpdateMem(newPtr, t.map2row(oldRow))
		for _, i := range updatedIndexes {
			if err := t.updateIndex(i, newPtr, newRow, oldRow); err != nil {
				panic(errors.Wrapf(err, "unexpected error while rollbacking index '%s'", i.Meta().Name))
			}
		}
	}

	return updateErr
}

func (t *Table) updateIndex(
	i *index.Index,
	newPtr allocator.Pointable,
	oldRow, newRow DataRow,
) error {
	t.deleteIndex(i, oldRow)

	if !t.canInsertIndex(i, newRow) {
		t.insertIndex(i, newPtr, oldRow)
		return fmt.Errorf("can't update row => %v", oldRow)
	}

	t.insertIndex(i, newPtr, newRow)
	return nil
}

func (t *Table) getAffectedIndexes(
	targetIndex *index.Index,
	row DataRow,
) map[string]*index.Index {
	indexesToUpdate := make(map[string]*index.Index, len(t.Indexes))

	for _, i := range t.Indexes {
		for col := range row {
			isPrimary := false
			if targetIndex.Meta().Name == t.Meta.PrimaryKey {
				isPrimary = true
			}

			colFound := false
			if !isPrimary {
				colFound = -1 != slices.IndexFunc(i.Columns(), func(c *column.Column) bool {
					return c.Name == col
				})
			}

			if isPrimary || colFound {
				indexesToUpdate[i.Meta().Name] = i
			}
		}
	}

	return indexesToUpdate
}
