package rbtree

type page[K, V EntryItem] struct {
	dirty       bool
	id          uint32
	size        uint16
	nodeNullPtr uint32
	entry       *Entry[K, V]

	nodes []*node[K, V]
}

func (p *page[K, V]) MarshalBinary() ([]byte, error) {
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

func (p *page[K, V]) UnmarshalBinary(d []byte) error {
	pageOffset := p.id * uint32(p.size)
	nodeSize := nodeFixedSize + p.entry.Size()
	for i := range p.nodes {
		e := p.entry.new()
		n := newNode(pageOffset+uint32(i*nodeSize), e)
		n.dirty = false

		err := n.UnmarshalBinary(d[i*nodeSize : (i+1)*nodeSize])
		if err != nil {
			return err
		}

		p.nodes[i] = n
	}
	return nil
}
