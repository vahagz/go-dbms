package cache

import (
	allocator "go-dbms/pkg/allocator/heap"

	"github.com/pkg/errors"
)

func NewCache[T any, U bmu[T]](size int) *Cache[T, U] {
	return &Cache[T, U]{
		size:  size,
		items: make(map[uint64]*pointerWrapper[T, U], size),
		keys:  make([]uint64, size),
		index: 0,
	}
}

type Cache[T any, U bmu[T]] struct {
	size  int
	items map[uint64]*pointerWrapper[T, U]
	keys  []uint64
	index int
}

func (c *Cache[T, U]) Add(ptr allocator.Pointable) Pointable[T, U] {
	addr := ptr.Addr()
	if itm, ok := c.items[addr]; ok {
		return itm
	}

	keyToDelete := c.keys[c.index]
	ptrToDelete := c.items[keyToDelete]
	if ptrToDelete.val.IsDirty() {
		err := ptrToDelete.ptr.Set(ptrToDelete.val)
		if err != nil {
			panic(errors.Wrap(err, "failed to flush ptr value to delete from cache"))
		}
	}

	delete(c.items, keyToDelete)
	c.keys[c.index] = addr
	c.items[addr] = &pointerWrapper[T, U]{cache: c, ptr: ptr}

	c.index++
	if c.index == c.size {
		c.index = 0
	}

	return c.items[addr]
}



// func (c *Cache[T, U]) Get(ptr allocator.Pointable) T {
// 	if itm, ok := c.items[ptr.Addr()]; ok {
// 		return itm
// 	}
// }
