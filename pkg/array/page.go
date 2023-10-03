package array

const pageHeaderSize = 2

func newPage[T elementer[U], U any](id uint64, meta *metadata) *page[T, U] {
	return &page[T, U]{
		id:    id,
		dirty: true,
		meta:  meta,
		elems: make([]T, 0),
	}
}

type page[T elementer[U], U any] struct {
	id    uint64
	dirty bool
	meta  *metadata
	elems []T
}

func (p *page[T, U]) element() T {
	var e U
	return T(&e)
}

func (p *page[T, U]) elementSize() uint16 {
	var a T
	return a.Size()
}

func (p *page[T, U]) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.meta.pageSize)
	offset := uint16(0)

	bin.PutUint16(buf[offset:offset+2], uint16(len(p.elems)))
	offset += 2

	elemSize := p.elementSize()
	for _, e := range p.elems {
		b, err := e.MarshalBinary()
		if err != nil {
			return nil, err
		}

		copy(buf[offset:offset+elemSize], b)
		offset += elemSize
	}

	return buf, nil
}

func (p *page[T, U]) UnmarshalBinary(d []byte) error {
	offset := uint16(0)

	count := bin.Uint16(d[offset : offset+2])
	offset += 2

	elemSize := p.elementSize()
	p.elems = make([]T, count)
	for i := range p.elems {
		p.elems[i] = p.element()
		err := p.elems[i].UnmarshalBinary(d[offset : offset+elemSize])
		if err != nil {
			return err
		}
		offset += elemSize
	}

	return nil
}
