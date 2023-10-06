package rbtree

func newPage(id uint64, meta *metadata) *page {
	return &page{
		dirty:       true,
		id:          id,
		size:        meta.pageSize,
		nodeKeySize: meta.nodeKeySize,
		nodeSize:    nodeFixedSize + meta.nodeKeySize,
		nodes:       make([]*node, meta.pageSize/(nodeFixedSize+meta.nodeKeySize)),
	}
}

type page struct {
	dirty       bool
	id          uint64
	size        uint16
	nodeKeySize uint16
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
	pageOffset := p.id * uint64(p.size)
	for i := range p.nodes {
		n := newNode(&pointer{
			raw:    pageOffset + uint64(i),
			pageId: p.id,
			index:  uint16(i),
		}, p.nodeKeySize)

		err := n.UnmarshalBinary(d[i*int(p.nodeSize) : (i+1)*int(p.nodeSize)])
		if err != nil {
			return err
		}

		p.nodes[i] = n
	}
	return nil
}
