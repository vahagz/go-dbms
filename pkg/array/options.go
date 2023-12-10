package array

type ArrayOptions struct {
	PageSize uint16
	PreAlloc uint16
}

type ScanOptions struct {
	Reverse bool
	Step    uint64
	Start   uint64
}
