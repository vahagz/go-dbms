package allocator

import "go-dbms/pkg/pager"

type Options struct {
	TargetPageSize uint16
	TreePageSize   uint16
	Pager          *pager.Pager
}
