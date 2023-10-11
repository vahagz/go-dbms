package rbtree

const metadataSize = 22

type metadata struct {
	dirty bool

	pageSize    uint16
	nodeKeySize uint16
	nodeValSize uint16
	top         uint32
	rootPtr     uint32
	nullPtr     uint32
	count       uint32
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	bin.PutUint16(buf[0:2], m.pageSize)
	bin.PutUint16(buf[2:4], m.nodeKeySize)
	bin.PutUint16(buf[4:6], m.nodeValSize)
	bin.PutUint32(buf[6:10], m.top)
	bin.PutUint32(buf[10:14], m.rootPtr)
	bin.PutUint32(buf[14:18], m.nullPtr)
	bin.PutUint32(buf[18:22], m.count)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.pageSize = bin.Uint16(d[0:2])
	m.nodeKeySize = bin.Uint16(d[2:4])
	m.nodeValSize = bin.Uint16(d[4:6])
	m.top = bin.Uint32(d[6:10])
	m.rootPtr = bin.Uint32(d[10:14])
	m.nullPtr = bin.Uint32(d[14:18])
	m.count = bin.Uint32(d[18:22])
	return nil
}
