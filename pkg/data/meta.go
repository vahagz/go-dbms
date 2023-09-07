package data

import (
	"errors"
	"log"
)

const (
	magic              = 0xD0D
	version            = uint8(0x1)
	metadataHeaderSize = 16
)

type column struct {
	name string
	typ  uint8
}

// metadata represents the metadata for the data file stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic    uint16   // magic marker to identify B+ tree.
	version  uint8    // version of implementation
	flags    uint8    // flags (unused)
	pageSz   uint32   // page size used to initialize
	columns  []column // list of columns
	freeList []int    // list of allocated, unused pages

	// metrics
	size int
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSz)

	// verify that the free list can fit inside the meta page.
	freeListSpace := int(m.pageSz) - metadataHeaderSize
	if len(m.freeList)*4 > freeListSpace {
		// TODO: make sure this doesn't happen by compacting pager
		// when free page count hits a threshold
		log.Printf("WARNING: truncating free list since it doesn't fit in meta page")
		m.freeList = m.freeList[:freeListSpace/4]
	}

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint32(buf[4:8], m.pageSz)
	bin.PutUint16(buf[8:10], uint16(len(m.columns)))

	offset := 10
	for i := 0; i < len(m.columns); i++ {
		colBytes := []byte(m.columns[i].name)

		buf[offset] = m.columns[i].typ
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(colBytes)))
		offset += 2

		copy(buf[offset:offset+len(colBytes)], colBytes)
		offset += len(colBytes)
	}

	bin.PutUint32(buf[offset:offset+4], uint32(len(m.freeList)))
	offset += 4
	for i := 0; i < len(m.freeList); i++ {
		bin.PutUint32(buf[offset:offset+4], uint32(m.freeList[i]))
		offset += 4
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
	m.pageSz = bin.Uint32(d[4:8])
	colLen := bin.Uint16(d[8:10])

	offset := 10
	m.columns = make([]column, colLen)

	for i := 0; i < int(colLen); i++ {
		m.columns[i].typ = uint8(d[offset])
		offset++

		colLen := int(bin.Uint16(d[offset:offset+2]))
		offset += 2

		m.columns[i].name = string(d[offset:offset+colLen])
		offset += colLen
	}

	m.freeList = make([]int, bin.Uint32(d[offset:offset+4]))
	for i := 0; i < len(m.freeList); i++ {
		m.freeList[i] = int(bin.Uint32(d[offset:offset+4]))
		offset += 4
	}

	return nil
}
