package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Find(filter *statement.WhereStatement) []index.Entry {
	var err error
	result := []index.Entry{}

	if t.meta.PrimaryKey == nil {
		err = t.df.Scan(func(ptr allocator.Pointable, r []types.DataType) (bool, error) {
			row := t.row2map(r)
			if filter == nil || filter.Compare(row) {
				result = append(result, index.Entry{
					Ptr: ptr,
					Row: row,
				})
			}
			return false, nil
		})
	} else {
		err = t.indexes[*t.meta.PrimaryKey].Scan(index.ScanOptions{
			ScanOptions: bptree.ScanOptions{
				Strict:  true,
			},
		}, func(key [][]byte, ptr allocator.Pointable) (bool, error) {
			row := t.get(ptr)
			if filter == nil || filter.Compare(row) {
				result = append(result, index.Entry{
					Ptr: ptr,
					Row: row,
				})
			}
			return false, nil
		})
	}

	if err != nil {
		panic(errors.Wrapf(err, "unexpected error while full scanning table"))
	}
	return result
}

func (t *Table) FindByIndex(name string, start, end *index.Filter, filter *statement.WhereStatement) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	index, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	result := []map[string]types.DataType{}
	return result, index.ScanFilter(start, end, func(ptr allocator.Pointable) (stop bool, err error) {
		row := t.get(ptr)
		if filter == nil || filter.Compare(row) {
			result = append(result, row)
		}
		return false, nil
	})
}

func (t *Table) FullScan(scanFn func(ptr allocator.Pointable, row map[string]types.DataType) (bool, error)) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return scanFn(ptr, t.row2map(row))
	})
}

func (t *Table) FullScanByIndex(
	indexName string,
	reverse bool,
	scanFn func(row map[string]types.DataType) (bool, error),
) error {
	idx, ok := t.indexes[indexName]
	if !ok {
		return fmt.Errorf("index not found => %v", indexName)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	return idx.Scan(index.ScanOptions{
		ScanOptions: bptree.ScanOptions{
			Reverse: reverse,
			Strict:  true,
		},
	}, func(key [][]byte, ptr allocator.Pointable) (bool, error) {
		return scanFn(t.get(ptr))
	})
}

func (t *Table) get(ptr allocator.Pointable) map[string]types.DataType {
	return t.df.GetMap(ptr)
}
