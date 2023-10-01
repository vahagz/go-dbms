package freelist

import (
	"bytes"
)

const (
	metadataHeaderSize = 16
	metadataItemSize   = 6
)

type metadata struct {
	dirty    bool
	pageSize uint16

	head         *Pointer
	preAlloc     uint16
	valSize      uint16
	next         uint32
	notFullPages map[uint32]uint16
}

func (m *metadata) isFull() bool {
	return len(m.notFullPages) == m.itemsPerPage()
}

func (m *metadata) itemsPerPage() int {
	return (int(m.pageSize) - metadataHeaderSize) / metadataItemSize
}

func (m *metadata) updatePageFreeCount(p *page) {
	m.dirty = true
	m.notFullPages[p.id]--
	if m.notFullPages[p.id] == 0 {
		delete(m.notFullPages, p.id)
	}
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSize)
	offset := 0

	if m.head != nil {
		bin.PutUint32(buf[offset:offset+4], m.head.PageId)
		offset += 4

		bin.PutUint16(buf[offset:offset+2], m.head.Index)
		offset += 2
	} else {
		offset += 6
	}

	bin.PutUint16(buf[offset:offset+2], uint16(m.preAlloc))
	offset += 2

	bin.PutUint16(buf[offset:offset+2], uint16(m.valSize))
	offset += 2

	bin.PutUint32(buf[offset:offset+4], m.next)
	offset += 4

	bin.PutUint16(buf[offset:offset+2], uint16(len(m.notFullPages)))
	offset += 2

	for pageId, freeCount := range m.notFullPages {
		bin.PutUint32(buf[offset:offset+4], pageId)
		offset += 4

		bin.PutUint16(buf[offset:offset+2], freeCount)
		offset += 2
	}

	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	zeroValue := make([]byte, 6)
	offset := 0

	if bytes.Equal(d[offset:offset+6], zeroValue) {
		m.head = nil
		offset += 6
	} else {
		m.head = &Pointer{}

		m.head.PageId = bin.Uint32(d[offset:offset+4])
		offset += 4

		m.head.Index = bin.Uint16(d[offset:offset+2])
		offset += 2
	}

	m.preAlloc = bin.Uint16(d[offset:offset+2])
	offset += 2

	m.valSize = bin.Uint16(d[offset:offset+2])
	offset += 2

	m.next = bin.Uint32(d[offset:offset+4])
	offset += 4

	count := bin.Uint16(d[offset:offset+2])
	offset += 2

	m.notFullPages = make(map[uint32]uint16, count)

	for i := uint16(0); i < count; i++ {
		pageId := bin.Uint32(d[offset:offset+4])
		offset += 4

		freeCount := bin.Uint16(d[offset:offset+2])
		offset += 2

		m.notFullPages[pageId] = freeCount
	}

	return nil
}
