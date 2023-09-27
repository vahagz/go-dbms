package freelist

const (
	metadataHeaderSize = 2
	metadataItemSize   = 6
)

func newMeta(pageSize uint16) *metadata {
	return &metadata{pageSize: pageSize}
}

type metadata struct {
	pageSize uint16

	next         uint32
	notFullPages map[uint32]uint16
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSize)
	offset := 0

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
	offset := 0

	m.next = bin.Uint32(d[offset : offset+4])
	offset += 4

	count := bin.Uint16(d[offset : offset+2])
	offset += 2

	m.notFullPages = make(map[uint32]uint16, count)

	for i := uint16(0); i < count; i++ {
		pageId := bin.Uint32(d[offset : offset+4])
		offset += 4

		freeCount := bin.Uint16(d[offset : offset+2])
		offset += 2

		m.notFullPages[pageId] = freeCount
	}

	return nil
}
