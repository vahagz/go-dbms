package freelist

import "bytes"

const itemSize = 22

func newPage(pageSize uint16) *page {
	return &page{pageSize: pageSize}
}

type value struct {
	pageId    uint64
	freeSpace uint64
}

type pointer struct {
	pageId uint32
	index  uint16
}

type item struct {
	val  *value
	next *pointer
}

type page struct {
	pageSize uint16

	free  []uint16
	items map[uint16]*item
}

func (p *page) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.pageSize)
	itemCount := p.pageSize / itemSize
	offset := uint16(0)

	for i := uint16(0); i < itemCount; i++ {
		item, ok := p.items[i]
		if !ok {
			offset += itemSize
			continue
		}

		bin.PutUint64(buf[offset:offset+8], item.val.pageId)
		offset += 8
		bin.PutUint64(buf[offset:offset+8], item.val.freeSpace)
		offset += 8
		bin.PutUint32(buf[offset:offset+4], item.next.pageId)
		offset += 4
		bin.PutUint16(buf[offset:offset+2], item.next.index)
		offset += 2
	}

	return buf, nil
}

func (p *page) UnmarshalBinary(d []byte) error {
	zeroValue := make([]byte, itemSize)
	itemCount := p.pageSize / itemSize
	offset := uint16(0)

	for i := uint16(0); i < itemCount; i++ {
		if bytes.Equal(d[offset:offset+itemSize], zeroValue) {
			p.free = append(p.free, i)
			offset += itemSize
			continue
		}

		itm := &item{
			val: &value{},
			next: &pointer{},
		}

		itm.val.pageId = bin.Uint64(d[offset:offset+8])
		offset += 8

		itm.val.freeSpace = bin.Uint64(d[offset:offset+8])
		offset += 8

		itm.next.pageId = bin.Uint32(d[offset:offset+8])
		offset += 4

		itm.next.index = bin.Uint16(d[offset:offset+8])
		offset += 2

		p.items[i] = itm
	}

	return nil
}
