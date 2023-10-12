package allocator

type Options struct {
	TargetPageSize       uint16
	TargetPageHeaderSize uint16
	TreePageSize         uint16
	Pager                Pager
	RemoveFunc           RemoveFunc
}
