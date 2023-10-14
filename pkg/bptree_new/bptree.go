package bptree

import "encoding/binary"

var bin = binary.BigEndian

type BPTree struct {
	root *node
}
