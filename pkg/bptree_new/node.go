package bptree

import (
	"encoding"
)

func newNode(id uint64) *node {
	return &node{dirty: true}
}

type Key interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Val interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Entry struct {
	Key Key
	Val Val
}

type node struct {
	dirty    bool

	next     uint64
	prev     uint64
	entries  []Entry
	children []uint64
}

func (n *node) IsDirty() bool {
	return n.dirty
}

func (n *node) Dirty(v bool) {
	n.dirty = v
}

func (n *node) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	return nil
}
