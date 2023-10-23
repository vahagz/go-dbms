package cache

import (
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/customerrors"
	"sync"

	"github.com/pkg/errors"
)

func NewCache[T any, U bmu[T]](size int, a *allocator.Allocator) *Cache[T, U] {
	return &Cache[T, U]{
		lock:   &sync.Mutex{},
		size:   size,
		items:  make(map[uint64]*pointerWrapper[T, U], size),
		locked: map[uint64]*pointerWrapper[T, U]{},
		keys:   make([]uint64, size),
		index:  0,
	}
}

type Cache[T any, U bmu[T]] struct {
	lock   *sync.Mutex
	a      allocator.Allocator
	size   int
	items  map[uint64]*pointerWrapper[T, U]
	locked map[uint64]*pointerWrapper[T, U]
	keys   []uint64
	index  int
}

func (c *Cache[T, U]) Pointer() Pointable[T, U] {
	return &pointerWrapper[T, U]{
		cache: c,
		ptr:   c.a.Pointer(0, 0),
		lock:  &sync.RWMutex{},
	}
}

func (c *Cache[T, U]) Add(ptr allocator.Pointable) Pointable[T, U] {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.add(ptr)
}

func (c *Cache[T, U]) add(ptr allocator.Pointable) Pointable[T, U] {
	addr := ptr.Addr()
	if itm, ok := c.items[addr]; ok {
		return itm
	} else if itm, ok := c.locked[addr]; ok {
		return itm
	}

	keyToDelete := c.keys[c.index]
	ptrToDelete, ok := c.items[keyToDelete]
	if ok {
		if ptrToDelete.val != nil && ptrToDelete.val.IsDirty() {
			err := ptrToDelete.ptr.Set(ptrToDelete.val)
			if err != nil {
				panic(errors.Wrap(err, "failed to flush ptr value to delete from cache"))
			}
		}
		delete(c.items, keyToDelete)
	}

	c.keys[c.index] = addr
	c.items[addr] = &pointerWrapper[T, U]{
		cache: c,
		ptr:   ptr,
		lock:  &sync.RWMutex{},
	}

	c.index++
	if c.index == c.size {
		c.index = 0
	}

	return c.items[addr]
}

func (c *Cache[T, U]) Get(ptr allocator.Pointable) (Pointable[T, U], error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.get(ptr)
}

func (c *Cache[T, U]) get(ptr allocator.Pointable) (Pointable[T, U], error) {
	addr := ptr.Addr()
	if itm, ok := c.items[addr]; ok {
		return itm, nil
	} else if itm, ok := c.locked[addr]; ok {
		return itm, nil
	}
	return nil, customerrors.ErrNotFound
}
