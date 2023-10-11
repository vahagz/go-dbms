package rbtree

import "encoding"

type Entry interface {
	New() Entry
	Size() int
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}
