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
	prefixColsCountStart := len(start.Conditions)
	prefixColsCountEnd := 0
	postfixColsCountStart := 0
	postfixColsCountEnd := 0
	var endKey [][]byte

	startVal := types.DataRow{}
	for _, cond := range start.Conditions {
		startVal[cond.Left.Alias] = eval.Eval(nil, cond.Right)
	}

	if end != nil {
		endVal := types.DataRow{}
		for _, cond := range end.Conditions {
			endVal[cond.Left.Alias] = eval.Eval(nil, cond.Right)
		}

		endKey = i.key(endVal)
		postfixColsCountEnd = len(endKey) - len(endVal)
		prefixColsCountEnd = len(end.Conditions)
	}

	for _, col := range i.columns {
		if _, ok := startVal[col.Name]; !ok {
			postfixColsCountStart++
			if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
				startVal[col.Name] = types.Type(col.Meta).Fill()
			} else {
				startVal[col.Name] = types.Type(col.Meta).Zero()
			}
		}
	}

	opts.Key = i.key(startVal)
	searchingKey := opts.Key
	if postfixColsCountStart > 0 {
		searchingKey = i.removeAutoSetCols(searchingKey, prefixColsCountStart, postfixColsCountStart)
	}
	if postfixColsCountEnd > 0 {
		endKey = i.removeAutoSetCols(endKey, prefixColsCountEnd, postfixColsCountEnd)
	}

	return i.tree.Scan(opts, func(k [][]byte, v []byte) (bool, error) {
		kStart := k
		kEnd := k
		if !i.tree.IsUniq() {
			k = i.tree.RemoveSuffix(k)
		}
		if postfixColsCountStart > 0 {
			kStart = i.removeAutoSetCols(k, prefixColsCountStart, postfixColsCountStart)
		}
		if postfixColsCountEnd > 0 {
			kEnd = i.removeAutoSetCols(k, prefixColsCountEnd, postfixColsCountEnd)
		}
		if shouldStop(kStart, start.Operator, searchingKey) || (endKey != nil && shouldStop(kEnd, end.Operator, endKey)) {
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
