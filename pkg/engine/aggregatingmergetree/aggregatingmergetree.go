package aggregatingmergetree

import (
	"go-dbms/pkg/engine/mergetree"
	"go-dbms/pkg/table"

	"github.com/pkg/errors"
)

type AggregatingMergeTree struct {
	*mergetree.MergeTree
	Meta IMetadata
}

func Open(opts *Options) (table.ITable, error) {
	if opts.Options.NewMeta == nil {
		opts.Options.NewMeta = func() table.IMetadata {
			return &Metadata{
				Metadata: &table.Metadata{},
			}
		}
	}

	t, err := mergetree.Open(opts.Options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open master table")
	}

	mt := t.(*mergetree.MergeTree)
	tree := &AggregatingMergeTree{
		MergeTree: mt,
		Meta:      mt.Meta.(IMetadata),
	}
	tree.MergeFn = tree.MergeTreeFn

	if tree.Meta.GetAggregations() == nil {
		tree.Meta.SetAggregations(opts.Aggregations)
	}

	return tree, nil
}
