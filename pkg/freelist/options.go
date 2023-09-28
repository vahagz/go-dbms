package freelist

type Options struct {
	Allocator        Allocator
	PreAlloc         int
	TargetPageSize   uint16
	FreelistPageSize uint16
}
