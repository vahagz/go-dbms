package bptree

import (
	"encoding/binary"
	allocator "go-dbms/pkg/allocator/heap"
)

var bin = binary.BigEndian

type BPTree struct {
	heap allocator.Allocator

	root    allocator.WrappedPointable[node, *node]
	metaPtr allocator.WrappedPointable[metadata, *metadata]
	meta    *metadata
	// nodes   map[uint64]
}
