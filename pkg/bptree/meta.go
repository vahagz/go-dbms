package bptree

import (
	allocator "go-dbms/pkg/allocator/heap"
)

const (
	magic        = 0xD0D
	version      = uint8(0x1)
	metadataSize = 20 + allocator.PointerSize
)

// metadata represents the metadata for the B+ tree stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic    uint16              // magic marker to identify B+ tree.
	version  uint8               // version of implementation
	flags    uint8               // flags (unused)
	keyCols  uint16              // columns count in key
	keySize  uint16              // maximum key size allowed
	valSize  uint16              // maximum value size allowed
	pageSize uint32              // page size used to initialize
	size     uint32              // number of entries in the tree
	degree   uint16              // number of entries per node
	root     allocator.Pointable // pointer to root node
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)
	rootPtrBytes, err := m.root.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.keyCols)
	bin.PutUint16(buf[6:8], m.keySize)
	bin.PutUint16(buf[8:10], m.valSize)
	bin.PutUint32(buf[10:14], m.pageSize)
	bin.PutUint32(buf[14:18], m.size)
	bin.PutUint16(buf[18:20], m.degree)
	copy(buf[20:], rootPtrBytes)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.magic = bin.Uint16(d[0:2])
	m.version = d[2]
	m.flags = d[3]
	m.keyCols = bin.Uint16(d[4:6])
	m.keySize = bin.Uint16(d[6:8])
	m.valSize = bin.Uint16(d[8:10])
	m.pageSize = bin.Uint32(d[10:14])
	m.size = bin.Uint32(d[14:18])
	m.degree = bin.Uint16(d[18:20])
	m.root.UnmarshalBinary(d[20:])
	return nil
}
