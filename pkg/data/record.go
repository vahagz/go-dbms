package data

import (
	"errors"
)

const recordHeaderSz = 1

// newrecord initializes an in-memory record and returns.
func newRecord(id int, meta metadata) *record {
	return &record{
		id:    id,
		dirty: true,
		meta: meta,
	}
}

// record represents a data row in the Data file.
type record struct {
	// configs for read/write
	dirty bool

	// record data
	id   int
	data [][]byte
	meta metadata
}

func (r record) size() int {
	sz := recordHeaderSz
	for i := 0; i < len(r.data); i++ {
		// 1 for the type size
		sz += 1 + len(r.data[i])
	}
	return sz
}

func (r record) MarshalBinary() ([]byte, error) {
	buf := make([]byte, r.size())
	offset := 0

	buf[0] = uint8(len(r.data))
	offset++

	for i := 0; i < len(r.data); i++ {
		data := r.data[i]
		if !isFixedSize(r.meta.columns[i].typ) {
			bin.PutUint16(buf[offset:offset+2], uint16(len(data)))
			offset += 2
		}

		copy(buf[offset:offset+len(data)], data)
		offset += len(data)
	}

	return buf, nil
}

func (r *record) UnmarshalBinary(d []byte) error {
	if r == nil {
		return errors.New("cannot unmarshal into nil record")
	}

	offset := 0

	// record
	colCount := int(d[0])
	offset++

	r.data = make([][]byte, colCount)

	for i := 0; i < colCount; i++ {
		size := 0
		if isFixedSize(r.meta.columns[i].typ) {
			size, _ = getSize(r.meta.columns[i].typ)
		} else {
			size = int(bin.Uint16(d[offset:offset+2]))
			offset += 2
		}

		r.data[i] = make([]byte, size)
		copy(r.data[i], d[offset:offset+size])
		offset += size
	}

	return nil
}
