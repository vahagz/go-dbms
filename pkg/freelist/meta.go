package freelist

import "bytes"

const (
	metadataHeaderSize = 12
	metadataItemSize   = 6
)

type metadata struct {
	dirty    bool
	pageSize uint16
	head     *Pointer

	next         uint32
	notFullPages map[uint32]uint16
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSize)
	offset := 0

	if m.head != nil {
		bin.PutUint32(buf[offset:offset+4], m.head.pageId)
		offset += 4

		bin.PutUint16(buf[offset:offset+2], m.head.index)
		offset += 2
	} else {
		offset += 6
	}

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

		m.head.pageId = bin.Uint32(d[offset:offset+4])
		offset += 4

		m.head.index = bin.Uint16(d[offset:offset+2])
		offset += 2
	}

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
