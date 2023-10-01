package freelist

type LinkedListOptions struct {
	PageSize uint16
	PreAlloc uint16
	ValSize  uint16
}

type LinkedFreelistOptions struct {
	PageSize   uint16
	Allocator  Allocator
	RemoveFunc RemoveFunc
}
