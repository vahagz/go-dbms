package data

import (
	"errors"
	"go-dbms/pkg/column"
	"go-dbms/pkg/types"
)

// record represents a data row in the Data file.
type record struct {
	// configs for read/write
	dirty bool

	// record data
	data    []types.DataType
	columns []*column.Column // list of columns
}

func (r *record) Copy() interface{} {
	cp := *r
	r.data = nil
	return &cp
}

func (r record) Size() int {
	sz := 0

	for i := 0; i < len(r.data); i++ {
		// 1 for the type code size
		sz += 1 + r.data[i].Size()

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
		size := data.Size()
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
	r.data = make([]types.DataType, len(r.columns))

	for i, column := range r.columns {
		size := 0
		v := types.Type(column.Meta)

		if v.IsFixedSize() {
			size = v.Size()
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
