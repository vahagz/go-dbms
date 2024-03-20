package mergetree

import (
	"container/heap"
	"os"
	"path/filepath"
	"sync"

	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/pkg/types/sorted"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

const (
	MasterTable = "master"
	partsPath   = "./parts"
)

type IMergeTree interface {
	table.ITable
	Merge()
}

func Open(opts *table.Options) (table.ITable, error) {
	t, err := table.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open master table")
	}

	tree := &MergeTree{
		Table:     t.(*table.Table),
		Parts:     map[string]*table.Table{},
		mergeLock: &sync.Mutex{},
	}
	tree.mergeFn = tree.MergeFn

	opts.MetaFilePath = ""
	opts.Meta = tree.Table.Meta

	return tree, tree.Init(opts)
}

type MergeTree struct {
	*table.Table
	Parts     map[string]*table.Table
	mergeFn   func(main, part table.ITable)
	mergeLock *sync.Mutex
}

func (t *MergeTree) Init(opts *table.Options) error {
	err := t.CreateDirs()
	if err != nil {
		return err
	}

	err = t.ReadMeta(opts)
	if err != nil {
		return err
	}

	err = t.ReadIndexes()
	if err != nil {
		return err
	}

	return t.ReadParts(opts)
}

func (t *MergeTree) CreateDirs() error {
	if err := t.Table.CreateDirs(); err != nil {
		return err
	}
	return helpers.CreateDir(t.partsPath())
}

func (t *MergeTree) ReadParts(opts *table.Options) error {
	dir, err := os.ReadDir(t.partsPath())
	if err != nil {
		return errors.Wrap(err, "failed to read parts directory")
	}

	for _, partDir := range dir {
		if !partDir.IsDir() {
			continue
		}

		opts.DataPath = filepath.Join(t.partsPath(), partDir.Name())
		part, err := table.Open(opts)
		if err != nil {
			return errors.Wrapf(err, "failed to open part: '%s'", partDir.Name())
		}

		t.Parts[partDir.Name()] = part.(*table.Table)
	}

	return nil
}

func (t *MergeTree) PartsIterator(yield func(name string, part *table.Table) bool) {
	if !yield("", t.Table) {
		return
	}

	for name, v := range t.Parts {
		if !yield(name, v) {
			return
		}
	}
}

func (t *MergeTree) Close() {
	for _, p := range t.Parts {
		p.Close()
	}
	t.Table.Close()
}

func (t *MergeTree) partsPath() string {
	return filepath.Join(t.DataPath, partsPath)
}


func Pipe(
	src map[string]stream.ReaderContinue[types.DataRow],
	dst stream.WriterContinue[types.DataRow],
	indexCols []string,
) {
	hp := &sorted.Heap[string]{
		Keys: indexCols,
	}

	for name, partStr := range src {
		row, ok := partStr.Pop()
		if !ok {
			delete(src, name)
		} else {
			heap.Push(hp, sorted.HeapItem[string]{
				Key: row,
				Val: name,
			})
			partStr.Continue(true)
		}
	}

	for len(src) > 0 {
		itm := heap.Pop(hp).(sorted.HeapItem[string])
		dst.Push(itm.Key)
		if !dst.ShouldContinue() {
			for _, partStr := range src {
				partStr.Continue(false)
			}
			break
		}

		row, ok := src[itm.Val].Pop()
		if !ok {
			delete(src, itm.Val)
		} else {
			heap.Push(hp, sorted.HeapItem[string]{
				Key: row,
				Val: itm.Val,
			})
			src[itm.Val].Continue(true)
		}
	}
}
