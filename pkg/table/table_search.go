package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"

	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

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
		row := t.get(ptr)
		for _, v := range key[:len(key) - 1] {
			fmt.Print(string(v), ",")
		}
		fmt.Print(key[len(key) - 1])
		fmt.Print(" | ")
		for _, dt := range row {
			fmt.Print(dt.Value(), " ")
		}
		fmt.Println()

		return scanFn(t.get(ptr))
	})
}

func (t *Table) get(ptr allocator.Pointable) map[string]types.DataType {
	return t.df.GetMap(ptr)
}
