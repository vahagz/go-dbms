package data

import (
	"errors"
	"log"
)

const (
	magic              = 0xD0D
	version            = uint8(0x1)
	metadataHeaderSize = 18
)

// type column struct {
// 	name string
// 	typ  types.TypeCode
// 	meta types.DataTypeMeta
// }

// metadata represents the metadata for the data file stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic    uint16            // magic marker to identify B+ tree.
	version  uint8             // version of implementation
	flags    uint8             // flags (unused)
	pageSz   uint16            // page size used to initialize
	freeList map[uint64]uint16 // list of allocated, unused pages

	// metrics
	size int
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSz)

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.pageSz)

	offset := 6

	// verify that the free list can fit inside the meta page.
	freeListSpace := int(m.pageSz) - metadataHeaderSize //- columnsSize
	if len(m.freeList)*4 > freeListSpace {
		// TODO: make sure this doesn't happen by compacting pager
		// when free page count hits a threshold
		log.Printf("WARNING: truncating free list since it doesn't fit in meta page")
		// m.freeList = m.freeList[:freeListSpace/4]
	}

	bin.PutUint32(buf[offset:offset+4], uint32(len(m.freeList)))
	offset += 4
	for id, freeSpace := range m.freeList {
		bin.PutUint64(buf[offset:offset+8], id)
		offset += 8

		bin.PutUint16(buf[offset:offset+2], freeSpace)
		offset += 2
	}

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
	m.pageSz = bin.Uint16(d[4:6])

	offset := 6

	freeListSize := bin.Uint32(d[offset:offset+4])
	m.freeList = make(map[uint64]uint16, freeListSize)
	offset += 4
	for i := 0; i < int(freeListSize); i++ {
		id := bin.Uint64(d[offset:offset+8])
		offset += 8

		m.freeList[id] = bin.Uint16(d[offset:offset+2])
		offset += 2
	}

	return nil
}
