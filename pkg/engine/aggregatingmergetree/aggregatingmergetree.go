package aggregatingmergetree

import "go-dbms/pkg/engine/mergetree"

type AggregatingMergeTree struct {
	mergetree.MergeTree
}
