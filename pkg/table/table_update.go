package table

import (
	"fmt"

	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
	"golang.org/x/exp/slices"
)

func (t *Table) UpdateByIndex(
	name string,
	start, end *index.Filter,
	filter *statement.WhereStatement,
	updateValuesMap map[string]types.DataType,
) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	updIndex, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	entries := updIndex.ScanEntries(start, end, filter)
	result := make([]map[string]types.DataType, 0, len(entries))
	indexesToUpdate := make(map[string]*index.Index, len(t.indexes))

	for _, i := range t.indexes {
		for col := range updateValuesMap {
			isPrimary := false
			if t.meta.PrimaryKey != nil && updIndex.Meta().Name == *t.meta.PrimaryKey {
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

	for _, e := range entries {
		updated := make(map[string]types.DataType, len(e.Row))
		for col, oldVal := range e.Row {
			if newVal, ok := updateValuesMap[col]; ok {
				updated[col] = newVal
			} else {
				updated[col] = oldVal
			}
		}

		if err := t.update(e.Ptr, e.Row, updated, indexesToUpdate); err != nil {
			return nil, errors.Wrapf(err, "failed to update table")
		}
		result = append(result, t.row2pk(e.Row))
	}

	return result, nil
}

func (t *Table) update(
	oldPtr allocator.Pointable,
	oldRow, newRow map[string]types.DataType,
	indexesToUpdate map[string]*index.Index,
) error {
	newPtr := t.df.UpdateMem(oldPtr, t.map2row(newRow))
	ptrUpdated := !oldPtr.Equal(newPtr) // pointer in datafile updated
	updatedIndexes := make([]*index.Index, 0, len(t.indexes))
	var updateErr error

	for name, i := range t.indexes {
		_, indexShouldUpdate := indexesToUpdate[name]
		if ptrUpdated || indexShouldUpdate {
			if updateErr = t.updateIndex(i, oldPtr, newPtr, oldRow, newRow); updateErr != nil {
				break
			}
			updatedIndexes = append(updatedIndexes, i)
		}
	}

	// rollback if error occurred
	if updateErr != nil {
		newPtr = t.df.UpdateMem(newPtr, t.map2row(oldRow))
		for _, i := range updatedIndexes {
			if err := t.updateIndex(i, newPtr, newPtr, newRow, oldRow); err != nil {
				panic(errors.Wrapf(err, "unexpected error while rollbacking index '%s'", i.Meta().Name))
			}
		}
	}

	return updateErr
}

func (t *Table) updateIndex(
	i *index.Index,
	oldPtr, newPtr allocator.Pointable,
	oldRow, newRow map[string]types.DataType,
) error {
	t.deleteIndex(i, oldPtr, oldRow)

	if !t.canInsertIndex(i, newRow) {
		t.insertIndex(i, newPtr, oldRow)
		return fmt.Errorf("can't update row => %v", oldRow)
	}

	t.insertIndex(i, newPtr, newRow)
	return nil
}
