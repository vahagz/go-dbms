package bptree

import (
	"encoding"
)

func newNode(id uint64) *node {
	return &node{
		id:    id,
		dirty: true,
	}
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
	id       uint64
	dirty    bool

	next     uint64
	prev     uint64
	entries  []Entry
	children []uint64
}
