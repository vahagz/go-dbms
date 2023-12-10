package allocator

const metadataSize = 8

type metadata struct {
	top uint64
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	bin.PutUint64(buf[0:8], m.top)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.top = bin.Uint64(d[0:8])
	return nil
}
