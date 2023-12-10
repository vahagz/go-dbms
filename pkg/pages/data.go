package pages

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
)

var bin = binary.BigEndian

type Slot interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Size() uint
	Copy() interface{}
}

// header length in page - 1 (flags) + 2 (slots count)
const dataPageHeaderSz = 3
// 6 is 2 + 2 + 2 (slot size + slot offset size + slot key size)
const SlotHeaderSz = 6

func NewData[T Slot](id uint64, pageSize uint16, dst T) *Data[T] {
	return &Data[T]{
		dst: dst,

		Dirty:     true,
		Id:        id,
		PageSize:  pageSize,
		freeSpace: pageSize - dataPageHeaderSz,
	}
}

// page represents a fixed size data block in file.
type Data[T Slot] struct {
	dst T

	Flags    uint8
	Dirty    bool
	PageSize uint16

	// page data
	Id        uint64
	slots     map[uint16]T
	freeSpace uint16
}

func (p *Data[T]) AddSlot(slot T) (uint16, error) {
	if uint(p.freeSpace) < slot.Size() + SlotHeaderSz {
		return 0, errors.New("not enough space for new slot")
	}

	key := p.newSlotKey()
	p.slots[key] = slot
	p.CalculateFreeSpace()
	return key, nil
}

func (p *Data[T]) RemoveSlot(slotKey uint16) error {
	if _, ok := p.slots[slotKey]; !ok {
		return fmt.Errorf("slot not found with key => %v", slotKey)
	}

	delete(p.slots, slotKey)
	p.CalculateFreeSpace()
	return nil
}

func (p *Data[T]) ClearSlots() {
	p.slots = map[uint16]T{}
	p.CalculateFreeSpace()
}

func (p *Data[T]) SlotCount() int {
	return len(p.slots)
}

func (p *Data[T]) Each(fn func(key uint16, slot T) (bool, error)) (bool, error) {
	var stop bool
	var err  error
	for k, v := range p.slots {
		stop, err = fn(k, v)
		if err != nil {
			return false, err
		} else if stop {
			return true, nil
		}
	}
	return false, nil
}

func (p *Data[T]) CalculateFreeSpace() {
	fs := p.PageSize - dataPageHeaderSz
	slotsSize := uint(0)

	for _, slot := range p.slots {
		slotsSize += slot.Size() + uint(SlotHeaderSz)
	}

	p.freeSpace = fs - uint16(slotsSize)
}

func (p *Data[T]) FreeSpace() uint16 {
	return p.freeSpace
}

func (p *Data[T]) Size() uint {
	sz := uint(dataPageHeaderSz)

	for _, s := range p.slots {
		sz += SlotHeaderSz + s.Size() // 2 (slot length size) + 2 (slot offset size) + 2 (slot key size) + slot size
	}

	return sz
}

func (p *Data[T]) MarshalBinary() ([]byte, error) {
	size := p.Size()
	buf := make([]byte, size)
	leftOffset := 0
	rightOffset := size

	buf[leftOffset] = p.Flags
	leftOffset++

	bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(len(p.slots)))
	leftOffset += 2

	for slotKey, slot := range p.slots {
		slotBytes, err := slot.MarshalBinary()
		if err != nil {
			return nil, err
		}

		bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(len(slotBytes)))
		leftOffset += 2

		bin.PutUint16(buf[leftOffset:leftOffset+2], uint16(rightOffset-uint(len(slotBytes))))
		leftOffset += 2

		bin.PutUint16(buf[leftOffset:leftOffset+2], slotKey)
		leftOffset += 2

		copy(buf[rightOffset-uint(len(slotBytes)):rightOffset], slotBytes)
		rightOffset -= uint(len(slotBytes))
	}

	return buf, nil
}

func (p *Data[T]) UnmarshalBinary(d []byte) error {
if p == nil {
		return errors.New("cannot unmarshal into nil page")
	}

	offset := 0

	p.Flags = d[offset]
	offset++

	slotCount := bin.Uint16(d[offset:offset+2])
	p.slots = make(map[uint16]T, slotCount)
	offset += 2

	for i := uint16(0); i < slotCount; i++ {
		slotSize := bin.Uint16(d[offset:offset+2])
		offset += 2

		slotOffset := bin.Uint16(d[offset:offset+2])
		offset += 2
		
		slotKey := bin.Uint16(d[offset:offset+2])
		offset += 2

		err := p.dst.UnmarshalBinary(d[slotOffset:slotOffset+slotSize])
		if err != nil {
			return err
		}

		p.slots[slotKey] = p.dst.Copy().(T)
	}

	p.CalculateFreeSpace()

	return nil
}

func (p *Data[T]) newSlotKey() uint16 {
	for k := uint16(1); k <= 0xffff; k++ {
		if _, ok := p.slots[k]; !ok {
			return k
		}
	}

	panic(fmt.Errorf("slots overflowed, page id => %v", p.Id))
}
