package freelist

import "bytes"

const itemSize = 16

func newPage(id uint32, pageSize uint16) *page {
	return &page{
		id:       id,
		dirty:    true,
		pageSize: pageSize,
		free:     []uint16{},
		items:    map[uint16]*item{},
	}
}

type value struct {
	pageId    uint64
	freeSpace uint16
}

type Pointer struct {
	PageId uint32
	Index  uint16
}

type item struct {
	val  *value
	next *Pointer
}

type page struct {
	id       uint32
	dirty    bool
	pageSize uint16

	free  []uint16
	items map[uint16]*item
}

func (p *page) init() {
	count := p.pageSize / itemSize
	p.free = make([]uint16, count)
	for i := uint16(0); i < count; i++ {
		p.free[i] = i
	}
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

		bin.PutUint16(buf[offset:offset+2], item.val.freeSpace)
		offset += 2

		if item.next != nil {
			bin.PutUint32(buf[offset:offset+4], item.next.PageId)
			offset += 4

			bin.PutUint16(buf[offset:offset+2], item.next.Index)
			offset += 2
		} else {
			offset += 6
		}
	}

	return buf, nil
}

func (p *page) UnmarshalBinary(d []byte) error {
	zeroValue := make([]byte, itemSize)
	ptrZeroValue := make([]byte, 6)
	itemCount := p.pageSize / itemSize
	offset := uint16(0)

	for i := uint16(0); i < itemCount; i++ {
		if bytes.Equal(d[offset:offset+itemSize], zeroValue) {
			p.free = append(p.free, i)
			offset += itemSize
			continue
		}

		itm := &item{
			val:  &value{},
			next: nil,
		}

		itm.val.pageId = bin.Uint64(d[offset:offset+8])
		offset += 8

		itm.val.freeSpace = bin.Uint16(d[offset:offset+2])
		offset += 2

		if bytes.Equal(d[offset:offset+6], ptrZeroValue) {
			offset += 6
		} else {
			itm.next = &Pointer{}

			itm.next.PageId = bin.Uint32(d[offset:offset+4])
			offset += 4
	
			itm.next.Index = bin.Uint16(d[offset:offset+2])
			offset += 2
		}

		p.items[i] = itm
	}

	return nil
}
