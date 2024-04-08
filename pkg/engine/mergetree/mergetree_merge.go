package mergetree

import (
	"fmt"
	"go-dbms/pkg/table"
	"go-dbms/util/helpers"
)

func (t *MergeTree) Merge() {
	if !t.mergeLock.TryLock() {
		return
	} else if len(t.Parts) == 0 {
		t.mergeLock.Unlock()
		return
	}

	go func() {
		fmt.Printf("[%s] starting merge\n", t.DataPath)
		defer fmt.Printf("[%s] merge finished\n", t.DataPath)
		defer t.mergeLock.Unlock()

		for name, part := range t.Parts {
			t.merge(part)
			delete(t.Parts, name)
		}
	}()
}

func (t *MergeTree) merge(part *table.Table) {
	t.MergeFn(t.Table, part)
	part.Drop()
}

func (t *MergeTree) MergeTreeFn(dst table.ITable, src table.ITable) {
	stream := helpers.MustVal(src.FullScanByIndex(t.PrimaryKey(), false))
	stream.AutoContinue(true)
	rs, eg := dst.Insert(stream)
	eg.Go(func() error {
		for _, ok := rs.Pop(); ok; _, ok = rs.Pop() {  }
		return nil
	})
	helpers.Must(eg.Wait())
}
