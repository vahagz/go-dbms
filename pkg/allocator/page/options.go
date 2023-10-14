package allocator

type Pager interface {
	Alloc(size int) (uint64, error)
}

type RemoveFunc func(pageId uint64, freeSpace uint16) bool

type Options struct {
	TargetPageSize       uint16
	TargetPageHeaderSize uint16
	TreePageSize         uint16
	Pager                Pager
	RemoveFunc           RemoveFunc
}
