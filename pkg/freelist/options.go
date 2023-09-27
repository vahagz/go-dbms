package freelist

type Options struct {
	allocator        Allocator
	targetPageSize   uint16
	freelistPageSize uint16
}
