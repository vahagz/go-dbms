package bptree

import (
	allocator "go-dbms/pkg/allocator/heap"

	"github.com/pkg/errors"
)

const metadataSize = 6 + allocator.PointerSize

type metadata struct {
	dirty bool

	maxKeySize   uint16              // maximum key size allowed
	maxValueSize uint16              // maximum value size allowed
	degree       uint16              // number of entries in the internal node
	root         allocator.Pointable // root node pointer
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSize)

	ptrBytes, err := m.root.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal root pointer")
	}

	bin.PutUint16(buf[0:2], m.maxKeySize)
	bin.PutUint16(buf[2:4], m.maxValueSize)
	bin.PutUint16(buf[4:6], m.degree)
	copy(buf[6:], ptrBytes)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	m.maxKeySize = bin.Uint16(d[0:2])
	m.maxValueSize = bin.Uint16(d[2:4])
	m.degree = bin.Uint16(d[4:6])
	return errors.Wrap(m.root.UnmarshalBinary(d[6:]), "failed to unmarshal root pointer")
}
