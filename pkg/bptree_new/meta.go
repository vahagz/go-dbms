package bptree

const metadataSize = 18

type metadata struct {
	dirty bool

	maxKeySize     uint16 // maximum key size allowed
	maxValueSize   uint16 // maximum value size allowed
	pageSize       uint16 // page size used to initialize
	internalDegree uint16 // number of entries in the internal node
	leafDegree     uint16 // number of entries in the leaf node
	root           uint64 // page id for the root node
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSize)
	bin.PutUint16(buf[0:2], m.maxKeySize)
	bin.PutUint16(buf[2:4], m.maxValueSize)
	bin.PutUint16(buf[4:6], m.pageSize)
	bin.PutUint16(buf[6:8], m.internalDegree)
	bin.PutUint16(buf[8:10], m.leafDegree)
	bin.PutUint64(buf[10:18], m.root)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.maxKeySize = bin.Uint16(d[0:2])
	m.maxValueSize = bin.Uint16(d[2:4])
	m.pageSize = bin.Uint16(d[4:6])
	m.internalDegree = bin.Uint16(d[6:8])
	m.leafDegree = bin.Uint16(d[8:10])
	m.root = bin.Uint64(d[10:18])
	return nil
}
