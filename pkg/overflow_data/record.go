package data

import (
	"errors"
	"math"
)

// header length in page 4 (overflow page id size)
const recordHeaderSz = 4

// newrecord initializes an in-memory record and returns.
func newRecord(id int, meta metadata) *record {
	return &record{
		id:       id,
		dirty:    true,
		meta:     meta,
		overflow: []int{},
	}
}

// record represents a data row in the Data file.
type record struct {
	// configs for read/write
	dirty bool

	// record data
	id       int
	overflow []int
	data     [][]byte
	meta     metadata
}

func (r record) size() int {
	sz := recordHeaderSz

	for i := 0; i < len(r.data); i++ {
		// 1 for the type size
		sz += 1 + len(r.data[i])
	}

	s := sz
	for s > 0 {
		if s > int(r.meta.pageSz) {
			sz += recordHeaderSz
			s += recordHeaderSz
		}
		s -= int(r.meta.pageSz)
	}

	return sz
}

func (r record) pageCount() int {
	return int(math.Ceil(float64(r.size()) / float64(r.meta.pageSz)))
}

func (r record) Overflows() []int {
	return r.overflow
}

func (r record) MarshalBinary() ([]byte, error) {
	size := r.size()
	buf := make([]byte, size)
	dataBuf := make([]byte, size - r.pageCount() * recordHeaderSz)
	offset := 0

	for j := 0; j < len(r.data); j++ {
		data := r.data[j]
		if !isFixedSize(r.meta.columns[j].typ) {
			bin.PutUint16(dataBuf[offset:offset+2], uint16(len(data)))
			offset += 2
		}

		copy(dataBuf[offset:offset+len(data)], data)
		offset += len(data)
	}

	offset = 0
	dataOffset := 0
	dataSize := int(r.meta.pageSz) - recordHeaderSz

	for _, oID := range r.overflow {
		bin.PutUint32(buf[offset:offset+4], uint32(oID))
		offset += 4

		copy(buf[offset:offset+dataSize], dataBuf[dataOffset:dataOffset+dataSize])

		offset += dataSize
		dataOffset += dataSize
	}

	offset += recordHeaderSz
	copy(buf[offset:], dataBuf[dataOffset:])
	return buf, nil
}

func (r *record) UnmarshalBinary(d []byte) error {
	if r == nil {
		return errors.New("cannot unmarshal into nil record")
	}

	data := make([]byte, len(d))
	offset := 0
	dataOffset := 0
	dataSize := int(r.meta.pageSz) - recordHeaderSz

	for offset < len(d) {
		offset += recordHeaderSz

		copy(data[dataOffset:dataOffset+dataSize], d[offset:offset+dataSize])
		offset += dataSize
		dataOffset += dataSize
	}

	r.data = make([][]byte, len(r.meta.columns))
	offset = 0

	for i, column := range r.meta.columns {
		size := 0
		if isFixedSize(column.typ) {
			size, _ = getSize(column.typ)
		} else {
			size = int(bin.Uint16(data[offset:offset+2]))
			offset += 2
		}

		r.data[i] = make([]byte, size)
		copy(r.data[i], data[offset:offset+size])
		offset += size
	}

	return nil
}

func (r *record) Next(d []byte) (int, error) {
	if r == nil {
		return 0, errors.New("cannot unmarshal into nil record")
	}

	nextID := int(bin.Uint32(d[0:4]))
	if nextID != 0 {
		r.overflow = append(r.overflow, nextID)
	}

	return nextID, nil
}
