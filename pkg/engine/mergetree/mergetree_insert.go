package mergetree

import (
	"path/filepath"

	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

func (t *MergeTree) Insert(in stream.Reader[types.DataRow]) (stream.Reader[types.DataRow], *errgroup.Group) {
	name, opts := t.newPartOpts()
	part := helpers.MustVal(table.Open(opts)).(*table.Table)
	t.mergeLock.Lock()
	t.Parts[name] = part
	t.mergeLock.Unlock()
	return part.Insert(in)
}

func (t *MergeTree) newPartOpts() (name string, opts *table.Options) {
	name = uuid.NewString()
	return name, &table.Options{
		DataPath: filepath.Join(t.partsPath(), name),
		Columns:  t.Table.Columns(),
		Engine:   t.Engine(),
		Meta:     t.Meta,
	}
}
