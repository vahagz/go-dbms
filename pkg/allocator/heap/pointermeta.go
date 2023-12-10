package allocator

const PointerMetaSize = 5

type pointerMetadata struct {
	free bool
	size uint32
}

func (m *pointerMetadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, PointerMetaSize)
	if m.free {
		buf[0] = 1
	}
	bin.PutUint32(buf[1:5], m.size)
	return buf, nil
}

func (m *pointerMetadata) UnmarshalBinary(d []byte) error {
	m.free = d[0] == 1
	m.size = bin.Uint32(d[1:5])
	return nil
}
