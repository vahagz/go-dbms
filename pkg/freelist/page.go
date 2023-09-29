package freelist

import "bytes"

const itemSize = 22

func newPage(id uint32, pageSize uint16) *page {
	return &page{
		id:       id,
		dirty:    true,
		pageSize: pageSize,
		free:     []uint16{},
		items:    map[uint16]*item{},
	}
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
			ptrBytes, err := item.next.MarshalBinary()
			if err != nil {
				return nil, err
			}
			copy(buf[offset:offset+PointerSize], ptrBytes)
			offset += PointerSize
		} else {
			offset += 6
		}
		
		if item.prev != nil {
			ptrBytes, err := item.prev.MarshalBinary()
			if err != nil {
				return nil, err
			}
			copy(buf[offset:offset+PointerSize], ptrBytes)
			offset += PointerSize
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
			if err := itm.next.UnmarshalBinary(d[offset:offset+PointerSize]); err != nil {
				return err
			}
			offset += 6
		}
		
		if bytes.Equal(d[offset:offset+6], ptrZeroValue) {
			offset += 6
		} else {
			itm.prev = &Pointer{}
			if err := itm.prev.UnmarshalBinary(d[offset:offset+PointerSize]); err != nil {
				return err
			}
			offset += 6
		}

		itm.self = &Pointer{
			PageId: p.id,
			Index:  i,
		}
		p.items[i] = itm
	}

	return nil
}
