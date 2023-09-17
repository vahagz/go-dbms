package data

import "fmt"

type RecordPointer struct {
	PageId int
	SlotId int
}

func (ptr RecordPointer) String() string {
	return fmt.Sprintf("ptr{page: %v, slot: %v}", ptr.PageId, ptr.SlotId)
}
