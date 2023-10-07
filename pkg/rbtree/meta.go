package rbtree

const metadataSize = 16

type metadata struct {
	dirty bool

	pageSize    uint16
	nodeKeySize uint16
	top         uint32
	rootPtr     uint32
	count       uint32
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	bin.PutUint16(buf[0:2], m.pageSize)
	bin.PutUint16(buf[2:4], m.nodeKeySize)
	bin.PutUint32(buf[4:8], m.top)
	bin.PutUint32(buf[8:12], m.rootPtr)
	bin.PutUint32(buf[12:16], m.count)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.pageSize = bin.Uint16(d[0:2])
	m.nodeKeySize = bin.Uint16(d[2:4])
	m.top = bin.Uint32(d[4:8])
	m.rootPtr = bin.Uint32(d[8:12])
	m.count = bin.Uint32(d[12:16])
	return nil
}
