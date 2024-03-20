package mergetree

import (
	"fmt"
	"sync"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"
)

func (t *MergeTree) Find(filter *statement.WhereStatement) stream.Reader[index.Entry] {
	s := stream.New[index.Entry](len(t.Parts))
	go func() {
		defer s.Close()
		wg := &sync.WaitGroup{}

		t.PartsIterator(func(_ string, part *table.Table) bool {
			wg.Add(1)
			go func(part *table.Table) {
				wg.Done()
				ps := part.Find(filter)
				for e, ok := ps.Pop(); ok; e, ok = ps.Pop() {
					s.Push(e)
				}
			}(part)
			return true
		})

		wg.Wait()
	}()
	return s
}

func (t *MergeTree) ScanByIndex(
	indexName string,
	start, end *index.Filter,
) (stream.ReaderContinue[types.DataRow], error) {
	if _, ok := t.Indexes[indexName]; !ok {
		return nil, fmt.Errorf("index not found => '%s'", indexName)
	}

	s := stream.New[types.DataRow](len(t.Parts))
	go func() {
		defer s.Close()
		sMap := make(map[string]stream.ReaderContinue[types.DataRow], len(t.Parts))

		t.PartsIterator(func(name string, part *table.Table) bool {
			sMap[name] = helpers.MustVal(part.ScanByIndex(indexName, start, end))
			return true
		})

		cols := append(t.Indexes[indexName].Meta().Columns, t.Indexes[t.PrimaryKey()].Meta().Columns...)
		Pipe(sMap, s, cols)
	}()
	return s, nil
}

func (t *MergeTree) FullScan() stream.ReaderContinue[types.DataRow] {
	s := stream.New[types.DataRow](len(t.Parts))
	go func() {
		defer s.Close()
		sMap := make(map[string]stream.ReaderContinue[types.DataRow], len(t.Parts))

		t.PartsIterator(func(name string, part *table.Table) bool {
			sMap[name] = part.FullScan()
			return true
		})

		Pipe(sMap, s, []string{})
	}()
	return s
}

func (t *MergeTree) FullScanByIndex(
	indexName string,
	reverse bool,
) (stream.ReaderContinue[types.DataRow], error) {
	if _, ok := t.Indexes[indexName]; !ok {
		return nil, fmt.Errorf("index not found => '%s'", indexName)
	}

	s := stream.New[types.DataRow](len(t.Parts))
	go func() {
		defer s.Close()
		sMap := make(map[string]stream.ReaderContinue[types.DataRow], len(t.Parts))

		t.PartsIterator(func(name string, part *table.Table) bool {
			sMap[name] = helpers.MustVal(part.FullScanByIndex(indexName, reverse))
			return true
		})

		cols := append(t.Indexes[indexName].Meta().Columns, t.Indexes[t.PrimaryKey()].Meta().Columns...)
		Pipe(sMap, s, cols)
	}()
	return s, nil
}
