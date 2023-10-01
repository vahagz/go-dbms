package freelist

import (
	"bytes"
	"fmt"
)

func newPage(id uint32, pageSize, valSize uint16) *page {
	return &page{
		id:       id,
		dirty:    true,
		pageSize: pageSize,
		valSize:  valSize,
		free:     []uint16{},
		items:    map[uint16]*item{},
	}
}

type page struct {
	id       uint32
	dirty    bool
	pageSize uint16
	valSize  uint16

	free  []uint16
	items map[uint16]*item
}

func (p *page) init() {
	count := p.pageSize / (itemHeaderSize + p.valSize)
	p.free = make([]uint16, count)
	p.dirty = true
	for i := uint16(0); i < count; i++ {
		p.free[i] = i
	}
}

func (p *page) isFull() bool {
	return len(p.free) == 0
}

func (p *page) add(val []byte) (*item, error) {
	if p.isFull() {
		return nil, fmt.Errorf("not enough free space in page => %v", p.id)
	}

	itmIndex := p.free[0]
	p.free = p.free[1:]
	p.dirty = true

	itm := &item{
		val: val,
		self: &Pointer{
			PageId: p.id,
			Index:  itmIndex,
		},
	}
	p.items[itmIndex] = itm

	return itm, nil
}

func (p *page) del(index uint16) {
	if _, ok := p.items[index]; !ok {
		return
	}
	delete(p.items, index)
	p.free = append(p.free, index)
	p.dirty = true
}

func (p *page) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.pageSize)
	itemSize := itemHeaderSize + p.valSize
	itemCount := p.pageSize / itemSize
	offset := uint16(0)

	for i := uint16(0); i < itemCount; i++ {
		item, ok := p.items[i]
		if !ok {
			offset += itemSize
			continue
		}

		copy(buf[offset:offset+p.valSize], item.val)
		offset += p.valSize

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
	itemSize := itemHeaderSize + p.valSize
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
			val: make([]byte, p.valSize),
		}

		itm.val = d[offset:offset+p.valSize]
		offset += p.valSize

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
