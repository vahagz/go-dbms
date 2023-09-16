package data

import (
	"errors"
	"go-dbms/pkg/types"
)

// no header in record
const recordHeaderSz = 0

// newrecord initializes an in-memory record and returns.
func newRecord(id int, meta *metadata) *record {
	return &record{
		id:    id,
		dirty: true,
		meta:  meta,
	}
}

// record represents a data row in the Data file.
type record struct {
	// configs for read/write
	dirty bool

	// record data
	id   int
	data []types.DataType
	meta *metadata
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
		sz += 1 + r.data[i].GetSize()

		if !r.data[i].IsFixedSize() {
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
		size := data.GetSize()
		if !data.IsFixedSize() {
			bin.PutUint16(buf[offset:offset+2], uint16(size))
			offset += 2
		}

		bytes, _ := data.MarshalBinary()
		copy(buf[offset:offset+size], bytes)
		offset += size
	}

	return buf, nil
}

func (r *record) UnmarshalBinary(d []byte) error {
	if r == nil {
		return errors.New("cannot unmarshal into nil record")
	}

	offset := 0
	r.data = make([]types.DataType, len(r.meta.columns))

	for i, column := range r.meta.columns {
		size := 0
		v := types.Type(types.TypeCode(column.typ))

		if v.IsFixedSize() {
			size = v.GetSize()
		} else {
			size = int(bin.Uint16(d[offset:offset+2]))
			offset += 2
		}

		v.UnmarshalBinary(d[offset:offset+size])
		r.data[i] = v
		offset += size
	}

	return nil
}
