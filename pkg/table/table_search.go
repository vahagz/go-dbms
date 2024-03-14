package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Find(filter *statement.WhereStatement) stream.Reader[index.Entry] {
	s := stream.New[index.Entry](0)
	go func() {
		defer s.Close()
		helpers.Must(t.Indexes[t.Meta.PrimaryKey].Scan(index.ScanOptions{
			ScanOptions: bptree.ScanOptions{
				Strict: true,
			},
		}, func(key [][]byte, ptr allocator.Pointable) (bool, error) {
			row := t.get(ptr)
			if filter == nil || filter.Compare(row) {
				s.Push(index.Entry{
					Ptr: ptr,
					Row: row,
				})
			}
			return false, nil
		}))
	}()
	return s
}

func (t *Table) ScanByIndex(
	name string,
	start, end *index.Filter,
) (stream.ReaderContinue[DataRow], error) {
	t.Mu.RLock()
	defer t.Mu.RUnlock()

	index, ok := t.Indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	s := stream.New[DataRow](0)
	go func() {
		defer s.Close()
		helpers.Must(index.ScanFilter(start, end, func(ptr allocator.Pointable) (stop bool, err error) {
			s.Push(t.get(ptr))
			return !s.ShouldContinue(), nil
		}))
	}()
	return s, nil
}

func (t *Table) FullScan() stream.ReaderContinue[DataRow] {
	t.Mu.RLock()
	defer t.Mu.RUnlock()

	s := stream.New[DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(t.DF.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
			s.Push(t.Row2map(row))
			return !s.ShouldContinue(), nil
		}))
	}()
	return s
}

func (t *Table) FullScanByIndex(
	indexName string,
	reverse bool,
) (stream.ReaderContinue[DataRow], error) {
	idx, ok := t.Indexes[indexName]
	if !ok {
		return nil, fmt.Errorf("index not found => %v", indexName)
	}

	t.Mu.RLock()
	defer t.Mu.RUnlock()

	s := stream.New[DataRow](0)
	go func ()  {
		defer s.Close()
		helpers.Must(idx.Scan(index.ScanOptions{
			ScanOptions: bptree.ScanOptions{
				Reverse: reverse,
				Strict:  true,
			},
		}, func(key [][]byte, ptr allocator.Pointable) (bool, error) {
			s.Push(t.get(ptr))
			return !s.ShouldContinue(), nil
		}))
	}()
	return s, nil
}

func (t *Table) get(ptr allocator.Pointable) DataRow {
	return t.DF.GetMap(ptr)
}
