package array

const metadataSize = 12

type metadata struct {
	dirty    bool
	pageSize uint16

	size     uint64
	preAlloc uint16
	elemSize uint16
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSize)
	bin.PutUint64(buf[0:8], m.size)
	bin.PutUint16(buf[8:10], m.preAlloc)
	bin.PutUint16(buf[10:12], m.elemSize)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.size = bin.Uint64(d[0:8])
	m.preAlloc = bin.Uint16(d[8:10])
	m.elemSize = bin.Uint16(d[10:12])
	return nil
}
