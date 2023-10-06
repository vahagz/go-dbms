package rbtree

type pointer struct {
	raw    uint64
	pageId uint64
	index  uint16
}
