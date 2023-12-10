package cache

import (
	"fmt"
	allocator "go-dbms/pkg/allocator/heap"
	"sync"
)

func NewCache[T pointable](size int, itemGenerator func() T) *Cache[T] {
	return &Cache[T]{
		mutex:   &sync.Mutex{},
		size:    size,
		items:   make(map[uint64]*pointerWrapper[T], size),
		locked:  map[uint64]*pointerWrapper[T]{},
		rlocked: map[uint64]*rlock[T]{},
		keys:    make([]uint64, size),
		index:   0,
		newItem: itemGenerator,
	}
}

type rlock[T pointable] struct {
	ptr     *pointerWrapper[T]
	readers int32
}

type Cache[T pointable] struct {
	mutex   *sync.Mutex
	size    int
	items   map[uint64]*pointerWrapper[T]
	locked  map[uint64]*pointerWrapper[T]
	rlocked map[uint64]*rlock[T]
	keys    []uint64
	index   int
	newItem func() T
}

func (c *Cache[T]) AddF(ptr allocator.Pointable, flag LOCKMODE) Pointable[T] {
	switch flag {
		case NONE:  return c.Add(ptr)
		case READ:  return c.AddR(ptr)
		case WRITE: return c.AddW(ptr)
	}
	panic(fmt.Errorf("invalid LOCKMODE value => %v", flag))
}

func (c *Cache[T]) Add(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.add(ptr)
}

func (c *Cache[T]) AddR(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	item := c.add(ptr)
	item.(*pointerWrapper[T]).lock().mutex.RLock()
	return item
}

func (c *Cache[T]) AddW(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	item := c.add(ptr)
	item.(*pointerWrapper[T]).lock().mutex.Lock()
	return item
}

func (c *Cache[T]) add(ptr allocator.Pointable) Pointable[T] {
	addr := ptr.Addr()
	if itm, ok := c.items[addr]; ok {
		return itm
	} else if itm, ok := c.locked[addr]; ok {
		return itm
	} else if itm, ok := c.rlocked[addr]; ok {
		return itm.ptr
	}

	keyToDelete := c.keys[c.index]
	ptrToDelete, ok := c.items[keyToDelete]
	if ok {
		ptrToDelete.Flush()
		delete(c.items, keyToDelete)
	}

	c.keys[c.index] = addr
	c.items[addr] = &pointerWrapper[T]{
		cache: c,
		ptr:   ptr,
		mutex: &sync.RWMutex{},
	}

	c.index++
	if c.index == c.size {
		c.index = 0
	}

	return c.items[addr]
}

func (c *Cache[T]) GetF(ptr allocator.Pointable, flag LOCKMODE) Pointable[T] {
	switch flag {
		case NONE:  return c.Get(ptr)
		case READ:  return c.GetR(ptr)
		case WRITE: return c.GetW(ptr)
	}
	panic(fmt.Errorf("invalid LOCKMODE value => %v", flag))
}

func (c *Cache[T]) Get(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.get(ptr)
}

func (c *Cache[T]) GetR(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	item := c.get(ptr)
	if item != nil {
		item.(*pointerWrapper[T]).lock().mutex.RLock()
	}
	return item
}

func (c *Cache[T]) GetW(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	item := c.get(ptr)
	if item != nil {
		item.(*pointerWrapper[T]).lock().mutex.Lock()
	}
	return item
}

func (c *Cache[T]) get(ptr allocator.Pointable) Pointable[T] {
	addr := ptr.Addr()
	if itm, ok := c.items[addr]; ok {
		return itm
	} else if itm, ok := c.locked[addr]; ok {
		return itm
	} else if itm, ok := c.rlocked[addr]; ok {
		return itm.ptr
	}
	return nil
}

func (c *Cache[T]) GetSet(ptr allocator.Pointable) Pointable[T] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if itm := c.get(ptr); itm == nil {
		return c.add(ptr)
	} else {
		return itm
	}
}

func (c *Cache[T]) Del(ptr allocator.Pointable) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.del(ptr)
}

func (c *Cache[T]) del(ptr allocator.Pointable) {
	delete(c.items, ptr.Addr())
	delete(c.locked, ptr.Addr())
	delete(c.rlocked, ptr.Addr())
}

func (c *Cache[T]) Flush() {
	for _, pw := range c.items {
		// pw.Lock().Flush().Unlock()
		pw.Flush()
	}

	for _, pw := range c.locked {
		pw.Flush()
	}
	
	for _, pw := range c.rlocked {
		pw.ptr.Flush()
	}
}

func (c *Cache[T]) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.items = make(map[uint64]*pointerWrapper[T], c.size)
	c.locked = map[uint64]*pointerWrapper[T]{}
	c.rlocked = map[uint64]*rlock[T]{}
	c.keys = make([]uint64, c.size)
}
