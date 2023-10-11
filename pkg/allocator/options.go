package freelist

type Options struct {
	TargetPageSize uint16
	TreePageSize   uint16
	Pager          Pager
	RemoveFunc     RemoveFunc
}
