package data

const (
	magic        = 0xD0D
	version      = uint8(0x1)
	metadataSize = 14
)

// metadata represents the metadata for the data file stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic    uint16 // magic marker to identify B+ tree.
	version  uint8  // version of implementation
	flags    uint8  // flags (unused)
	pageSize uint16 // page size used to initialize
	count    uint64 // count of entries
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.pageSize)
	bin.PutUint64(buf[6:14], m.count)

	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.magic = bin.Uint16(d[0:2])
	m.version = d[2]
	m.flags = d[3]
	m.pageSize = bin.Uint16(d[4:6])
	m.count = bin.Uint64(d[6:14])
	return nil
}
