package pager

import (
	"encoding"
	"errors"
)

type Slot interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Size() int
	Copy() interface{}
}

// header length in page - 1 (flags) + 2 (slots count)
const pageHeaderSz = 3

func NewPage[T Slot](id, PageSize int, dst T) *Page[T] {
	return &Page[T]{
		dst: dst,

		Dirty:     true,
		Id:        id,
		PageSize:  PageSize,
		FreeSpace: PageSize - pageHeaderSz,
	}
}

// page represents a fixed size data block in file.
type Page[T Slot] struct {
	dst T

	Flags    uint8
	Dirty    bool
	PageSize int

	// page data
	Id        int
	Slots     []T
	FreeSpace int
}

func (p *Page[T]) AddSlot(slot T) (int, error) {
	// 4 is 2 + 2 (slot size + slot offset size)
	if p.FreeSpace < slot.Size() + 4 {
		return -1, errors.New("not enough space for new slot")
	}
	p.Slots = append(p.Slots, slot)
	p.CalculateFreeSpace()
	return len(p.Slots) - 1, nil
}

func (p *Page[T]) ClearSlots() {
	// 4 is 2 + 2 (slot size + slot offset size)
	p.Slots = []T{}
	p.CalculateFreeSpace()
}

func (p *Page[T]) CalculateFreeSpace() {
	fs := p.PageSize - pageHeaderSz
	slotsSize := 0

	for _, slot := range p.Slots {
		// 4 is 2 + 2 (slot size + slot offset size)
		slotsSize += slot.Size() + 4
	}

	p.FreeSpace = fs - slotsSize
}

func (p Page[T]) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.PageSize)
	leftOffset := 0
	rightOffset := p.PageSize

	buf[leftOffset] = p.Flags
	leftOffset++

	bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(len(p.Slots)))
	leftOffset += 2

	for _, slot := range p.Slots {
		slotBytes, err := slot.MarshalBinary()
		if err != nil {
			return nil, err
		}

		bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(len(slotBytes)))
		leftOffset += 2

		bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(rightOffset-len(slotBytes)))
		leftOffset += 2

		copy(buf[rightOffset-len(slotBytes):rightOffset], slotBytes)
		rightOffset -= len(slotBytes)
	}

	return buf, nil
}

func (p *Page[T]) UnmarshalBinary(d []byte) error {
	if p == nil {
		return errors.New("cannot unmarshal into nil page")
	}
	if len(d) != p.PageSize {
		return errors.New("invalid binary size")
	}

	offset := 0

	p.Flags = d[offset]
	offset++

	p.Slots = make([]T, bin.Uint16(d[offset:offset+2]))
	offset += 2

	for i := range p.Slots {
		slotSize := bin.Uint16(d[offset:offset+2])
		offset += 2

		slotOffset := bin.Uint16(d[offset:offset+2])
		offset += 2

		err := p.dst.UnmarshalBinary(d[slotOffset:slotOffset+slotSize])
		if err != nil {
			return err
		}

		p.Slots[i] = p.dst.Copy().(T)
	}

	p.CalculateFreeSpace()

	return nil
}
