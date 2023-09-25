package data

import "fmt"

const RecordPointerSize = 10

type RecordPointer struct {
	PageId uint64
	SlotId uint16
}

func (ptr *RecordPointer) String() string {
	return fmt.Sprintf("ptr{page: %v, slot: %v}", ptr.PageId, ptr.SlotId)
}

func (ptr *RecordPointer) Size() int {
	return RecordPointerSize // (8 + 2) data page size + slot index size
}

func (ptr *RecordPointer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, RecordPointerSize)
	bin.PutUint64(buf[0:8], ptr.PageId)
	bin.PutUint16(buf[8:10], ptr.SlotId)
	return buf, nil
}

func (ptr *RecordPointer) UnmarshalBinary(d []byte) error {
	ptr.PageId = bin.Uint64(d[0:8])
	ptr.SlotId = bin.Uint16(d[8:10])
	return nil
}
