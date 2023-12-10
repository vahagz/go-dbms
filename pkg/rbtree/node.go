package rbtree

import "github.com/pkg/errors"

const nodeFixedSize = 13

func newNode[K, V EntryItem](ptr uint32, e *Entry[K, V]) *node[K, V] {
	return &node[K, V]{
		dirty: true,
		ptr:   ptr,
		entry: e,
		flags: FV_COLOR_RED,
	}
}

type flagVaue byte

const (
	FV_COLOR_BLACK flagVaue = 0b00000000
	FV_COLOR_RED   flagVaue = 0b00000001
)

type flagType byte

const (
	FT_COLOR flagType = 0
)

type node[K, V EntryItem] struct {
	dirty bool
	ptr   uint32

	left   uint32
	right  uint32
	parent uint32
	entry  *Entry[K, V]
	flags  flagVaue
}

func (n *node[K, V]) isBlack() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_BLACK
}

func (n *node[K, V]) isRed() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_RED
}

func (n *node[K, V]) setBlack() {
	n.dirty = true
	n.setFlag(FT_COLOR, FV_COLOR_BLACK)
}

func (n *node[K, V]) setRed() {
	n.dirty = true
	n.setFlag(FT_COLOR, FV_COLOR_RED)
}

func (n *node[K, V]) setFlag(ft flagType, fv flagVaue) {
	n.dirty = true
	mask := ^(byte(1) << ft)
	mask &= byte(n.flags)
	n.flags = flagVaue(mask) | fv
}

func (n *node[K, V]) getFlag(ft flagType) flagVaue {
	return n.flags & flagVaue(byte(1)<<byte(ft))
}

func (n *node[K, V]) MarshalBinary() ([]byte, error) {
	buf := make([]byte, nodeFixedSize + n.entry.Size())
	bin.PutUint32(buf[0:4], n.left)
	bin.PutUint32(buf[4:8], n.right)
	bin.PutUint32(buf[8:12], n.parent)
	buf[12] = byte(n.flags)

	b, err := n.entry.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal entry")
	}

	copy(buf[13:], b)
	return buf, nil
}

func (n *node[K, V]) UnmarshalBinary(d []byte) error {
	n.left = bin.Uint32(d[0:4])
	n.right = bin.Uint32(d[4:8])
	n.parent = bin.Uint32(d[8:12])
	n.flags = flagVaue(d[12])
	n.entry.UnmarshalBinary(d[13:])
	return nil
}
