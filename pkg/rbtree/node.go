package rbtree

const nodeFixedSize = 25

func newNode(ptr *pointer, keySize uint16) *node {
	return &node{
		dirty: true,
		ptr:   ptr,
		size:  nodeFixedSize + keySize,
		key:   make([]byte, keySize),
	}
}

type color byte

const (
	NODE_RED color = iota
	NODE_BLACK
)

type node struct {
	dirty bool
	ptr   *pointer
	size  uint16

	left   uint64
	right  uint64
	parent uint64
	color  color
	key    []byte
}

func (n *node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.size)
	bin.PutUint64(buf[0:8], n.left)
	bin.PutUint64(buf[8:16], n.right)
	bin.PutUint64(buf[16:32], n.parent)
	buf[32] = byte(n.color)
	copy(buf[32:], n.key)
	return buf, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	n.left = bin.Uint64(d[0:8])
	n.right = bin.Uint64(d[8:16])
	n.parent = bin.Uint64(d[16:32])
	n.color = color(d[32])
	copy(n.key, d[32:])
	return nil
}
