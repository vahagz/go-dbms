package rbtree

const nodeFixedSize = 13

func newNode(ptr uint32, keySize uint16) *node {
	return &node{
		dirty: true,
		ptr:   ptr,
		size:  nodeFixedSize + keySize,
		key:   make([]byte, keySize),
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

type node struct {
	dirty bool
	ptr   uint32
	size  uint16

	left   uint32
	right  uint32
	parent uint32
	key    []byte
	flags  flagVaue
}

func (n *node) isBlack() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_BLACK
}

func (n *node) isRed() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_RED
}

func (n *node) setBlack() {
	n.dirty = true
	n.setFlag(FT_COLOR, FV_COLOR_BLACK)
}

func (n *node) setRed() {
	n.dirty = true
	n.setFlag(FT_COLOR, FV_COLOR_RED)
}

func (n *node) setFlag(ft flagType, fv flagVaue) {
	n.dirty = true
	mask := ^(byte(1) << ft)
	mask &= byte(n.flags)
	n.flags = flagVaue(mask) | fv
}

func (n *node) getFlag(ft flagType) flagVaue {
	return n.flags & flagVaue(byte(1)<<byte(ft))
}

func (n *node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.size)
	bin.PutUint32(buf[0:4], n.left)
	bin.PutUint32(buf[4:8], n.right)
	bin.PutUint32(buf[8:12], n.parent)
	buf[12] = byte(n.flags)
	copy(buf[13:], n.key)
	return buf, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	n.left = bin.Uint32(d[0:4])
	n.right = bin.Uint32(d[4:8])
	n.parent = bin.Uint32(d[8:12])
	n.flags = flagVaue(d[12])
	copy(n.key, d[13:])
	return nil
}
