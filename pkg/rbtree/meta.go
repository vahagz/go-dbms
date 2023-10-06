package rbtree

const metadataSize = 20

type metadata struct {
	dirty bool

	pageSize    uint16
	nodeKeySize uint16
	top         uint64
	rootPtr     uint64
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	bin.PutUint16(buf[0:2], m.pageSize)
	bin.PutUint16(buf[2:4], m.nodeKeySize)
	bin.PutUint64(buf[4:12], m.top)
	bin.PutUint64(buf[12:20], m.rootPtr)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.pageSize = bin.Uint16(d[0:2])
	m.nodeKeySize = bin.Uint16(d[2:4])
	m.top = bin.Uint64(d[4:12])
	m.rootPtr = bin.Uint64(d[12:20])
	return nil
}
