package rbtree

type pointer struct {
	raw    uint32
	pageId uint32
	index  uint16
}
