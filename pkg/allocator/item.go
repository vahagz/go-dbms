package allocator

import (
	"encoding/binary"
	"go-dbms/pkg/rbtree"
)

var bin = binary.BigEndian

func newEntry[T initter](
	freeSpace uint16,
	pageId uint64,
) *rbtree.Entry[T, *rbtree.DummyVal] {
	return &rbtree.Entry[T, *rbtree.DummyVal]{
		Key: newKey[T](freeSpace, pageId),
		Val: &rbtree.DummyVal{},
	}
}

func newKey[T initter](freeSpace uint16, pageId uint64) T {
	var i T
	k := i.New().(initter)
	k.init(freeSpace, pageId)
	return k.(T)
}

func swap[T, F swapper, V rbtree.EntryItem](
	entry *rbtree.Entry[F, V],
) *rbtree.Entry[T, V] {
	var v rbtree.EntryItem = &rbtree.DummyVal{}
	return &rbtree.Entry[T, V]{
		Key: entry.Key.swap().(T),
		Val: v.(V),
	}
}

type swapper interface {
	rbtree.EntryItem
	swap() rbtree.EntryItem
}

type initter interface {
	rbtree.EntryItem
	init(freeSpace uint16, pageId uint64)
}

type ItemFP struct {
	FreeSpace uint16
	PageId    uint64
}

func (i *ItemFP) swap() rbtree.EntryItem {
	return (*ItemPF)(i)
}

func (i *ItemFP) init(freeSpace uint16, pageId uint64) {
	i.FreeSpace = freeSpace
	i.PageId = pageId
}

func (i *ItemFP) New() rbtree.EntryItem {
	return &ItemFP{}
}

func (i *ItemFP) Copy() rbtree.EntryItem {
	return &ItemFP{i.FreeSpace, i.PageId}
}

func (i *ItemFP) Size() int {
	return 10
}

func (i *ItemFP) IsNil() bool {
	return i == nil
}

func (i *ItemFP) MarshalBinary() ([]byte, error) {
	buf := make([]byte, i.Size())
	bin.PutUint16(buf[0:2], i.FreeSpace)
	bin.PutUint64(buf[2:10], i.PageId)
	return buf, nil
}

func (i *ItemFP) UnmarshalBinary(d []byte) error {
	i.FreeSpace = bin.Uint16(d[0:2])
	i.PageId = bin.Uint64(d[2:10])
	return nil
}




type ItemPF ItemFP

func (i *ItemPF) swap() rbtree.EntryItem {
	return (*ItemFP)(i)
}

func (i *ItemPF) init(freeSpace uint16, pageId uint64) {
	(*ItemFP)(i).init(freeSpace, pageId)
}

func (i *ItemPF) New() rbtree.EntryItem {
	return &ItemPF{}
}

func (i *ItemPF) Copy() rbtree.EntryItem {
	return (*ItemPF)(&ItemFP{i.FreeSpace, i.PageId})
}

func (i *ItemPF) Size() int {
	return (*ItemFP)(i).Size()
}

func (i *ItemPF) IsNil() bool {
	return (*ItemFP)(i).IsNil()
}

func (i *ItemPF) MarshalBinary() ([]byte, error) {
	buf := make([]byte, (*ItemFP)(i).Size())
	bin.PutUint64(buf[0:8], i.PageId)
	bin.PutUint16(buf[8:10], i.FreeSpace)
	return buf, nil
}

func (i *ItemPF) UnmarshalBinary(d []byte) error {
	i.PageId = bin.Uint64(d[0:8])
	i.FreeSpace = bin.Uint16(d[8:10])
	return nil
}
