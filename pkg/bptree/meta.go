package bptree

import (
	"errors"
)

const (
	magic              = 0xD0D
	version            = uint8(0x1)
	metadataHeaderSize = 24
)

// metadata represents the metadata for the B+ tree stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic        uint16 // magic marker to identify B+ tree.
	version      uint8  // version of implementation
	flags        uint8  // flags (unused)
	maxKeySz     uint16 // maximum key size allowed
	maxValueSz   uint16 // maximum value size allowed
	pageSz       uint32 // page size used to initialize
	size         uint32 // number of entries in the tree
	rootID       uint32 // page id for the root node
	preAlloc     uint16 // page count to alloc if no enough space
	targetPageSz uint16 // target data structure page size for freelist
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSz)

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.maxKeySz)
	bin.PutUint16(buf[6:8], m.maxValueSz)
	bin.PutUint32(buf[8:12], m.pageSz)
	bin.PutUint32(buf[12:16], m.size)
	bin.PutUint32(buf[16:20], m.rootID)
	bin.PutUint16(buf[20:22], m.preAlloc)
	bin.PutUint16(buf[22:24], m.targetPageSz)

	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	if len(d) < metadataHeaderSize {
		return errors.New("in-sufficient data for unmarshal")
	} else if m == nil {
		return errors.New("cannot unmarshal into nil")
	}

	m.magic = bin.Uint16(d[0:2])
	m.version = d[2]
	m.flags = d[3]
	m.maxKeySz = bin.Uint16(d[4:6])
	m.maxValueSz = bin.Uint16(d[6:8])
	m.pageSz = bin.Uint32(d[8:12])
	m.size = bin.Uint32(d[12:16])
	m.rootID = bin.Uint32(d[16:20])
	m.preAlloc = bin.Uint16(d[20:22])
	m.targetPageSz = bin.Uint16(d[22:24])

	return nil
}
