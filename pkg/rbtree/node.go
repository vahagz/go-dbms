package rbtree

const nodeFixedSize = 13

func newNode(ptr *pointer, keySize uint16) *node {
	return &node{
		dirty: true,
		ptr:   ptr,
		size:  nodeFixedSize + keySize,
		key:   make([]byte, keySize),
		color: NODE_COLOR_RED,
	}
}

type color byte

const (
	NODE_COLOR_RED color = iota
	NODE_COLOR_BLACK
)

type node struct {
	dirty bool
	ptr   *pointer
	size  uint16

	left   uint32
	right  uint32
	parent uint32
	color  color
	key    []byte
}

func (n *node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.size)
	bin.PutUint32(buf[0:4], n.left)
	bin.PutUint32(buf[4:8], n.right)
	bin.PutUint32(buf[8:12], n.parent)
	buf[12] = byte(n.color)
	copy(buf[13:], n.key)
	return buf, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	n.left = bin.Uint32(d[0:4])
	n.right = bin.Uint32(d[4:8])
	n.parent = bin.Uint32(d[8:12])
	n.color = color(d[12])
	copy(n.key, d[13:])
	return nil
}
