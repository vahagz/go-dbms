package index

import (
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (i *Index) Scan(
	opts ScanOptions,
	scanFn func(key [][]byte, ptr allocator.Pointable) (bool, error),
) error {
	return i.tree.Scan(opts.ScanOptions, func(key [][]byte, val []byte) (bool, error) {
		ptr := i.df.Pointer()
		ptr.UnmarshalBinary(val)
		return scanFn(key, ptr)
	})
}

func (i *Index) ScanFilter(start, end *Filter, scanFn func(ptr allocator.Pointable) (stop bool, err error)) error {
	opts := operatorMapping[start.Operator].scanOption
	// prefixColsCount := len(start.Value)
	prefixColsCount := 1
	postfixColsCount := 0

	startVal := map[string]types.DataType{
		start.Left.Alias: eval.Eval(nil, start.Right),
	}
	endVal := map[string]types.DataType{
		end.Left.Alias: eval.Eval(nil, end.Right),
	}

	for _, col := range i.columns {
		if _, ok := startVal[col.Name]; !ok {
			postfixColsCount++
			if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
				startVal[col.Name] = types.Type(col.Meta).Fill()
			} else {
				startVal[col.Name] = types.Type(col.Meta).Zero()
			}
		}
	}

	var endKey [][]byte
	if end != nil {
		endKey = i.key(endVal)
	}

	opts.Key = i.key(startVal)
	searchingKey := opts.Key
	if postfixColsCount > 0 {
		searchingKey = i.removeAutoSetCols(searchingKey, prefixColsCount, postfixColsCount)
	}

	return i.tree.Scan(opts, func(k [][]byte, v []byte) (bool, error) {
		if !i.tree.IsUniq() {
			k = i.tree.RemoveSuffix(k)
		}
		if postfixColsCount > 0 {
			k = i.removeAutoSetCols(k, prefixColsCount, postfixColsCount)
		}
		if shouldStop(k, start.Operator, searchingKey) || (endKey != nil && shouldStop(k, end.Operator, endKey)) {
			return true, nil
		}

		ptr := i.df.Pointer()
		if err := ptr.UnmarshalBinary(v); err != nil {
			return false, err
		}
		return scanFn(ptr)
	})
}

func (i *Index) ScanEntries(start *Filter, end *Filter, filter *statement.WhereStatement) []Entry {
	entries := []Entry{}

	err := i.ScanFilter(start, end, func(ptr allocator.Pointable) (stop bool, err error) {
		row := i.df.GetMap(ptr)
		if filter == nil || filter.Compare(row) {
			entries = append(entries, Entry{ptr, row})
		}
		return false, nil
	})
	if err != nil {
		panic(errors.Wrapf(err, "error while scanning index '%s'", i.meta.Name))
	}

	return entries
}
