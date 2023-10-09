package rbtree

func newPage(id uint32, meta *metadata) *page {
	return &page{
		dirty:       true,
		id:          id,
		size:        meta.pageSize,
		nodeKeySize: meta.nodeKeySize,
		nodeNullPtr: meta.nullPtr,
		nodeSize:    nodeFixedSize + meta.nodeKeySize,
		nodes:       make([]*node, meta.pageSize/(nodeFixedSize+meta.nodeKeySize)),
	}
}

type page struct {
	dirty       bool
	id          uint32
	size        uint16
	nodeKeySize uint16
	nodeNullPtr uint32
	nodeSize    uint16

	nodes []*node
}

func (p *page) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.size)
	for i, n := range p.nodes {
		if b, err := n.MarshalBinary(); err != nil {
			return nil, err
		} else {
			copy(buf[i*len(b):(i+1)*len(b)], b)
		}
	}
	return buf, nil
}

func (p *page) UnmarshalBinary(d []byte) error {
	pageOffset := p.id * uint32(p.size)
	for i := range p.nodes {
		n := newNode(pageOffset+uint32(i*int(p.nodeSize)), p.nodeKeySize)
		n.dirty = false

		err := n.UnmarshalBinary(d[i*int(p.nodeSize) : (i+1)*int(p.nodeSize)])
		if err != nil {
			return err
		}

		p.nodes[i] = n
	}
	return nil
}
