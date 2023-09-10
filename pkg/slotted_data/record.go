package data

import (
	"errors"
)

// no header in record
const recordHeaderSz = 0

// newrecord initializes an in-memory record and returns.
func newRecord(id int, meta metadata) *record {
	return &record{
		id:       id,
		dirty:    true,
		meta:     meta,
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

func (r *record) Copy() interface{} {
	cp := *r
	r.data = nil
	return &cp
}

func (r record) Size() int {
	sz := recordHeaderSz

	for i := 0; i < len(r.data); i++ {
		// 1 for the type size
		sz += 1 + len(r.data[i])

		if !isFixedSize(r.meta.columns[i].typ) {
			sz += 2
		}
	}

	return sz
}

func (r record) MarshalBinary() ([]byte, error) {
	buf := make([]byte, r.Size())
	offset := 0

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
	r.data = make([][]byte, len(r.meta.columns))

	for i, column := range r.meta.columns {
		size := 0
		if isFixedSize(column.typ) {
			size, _ = getSize(column.typ)
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
