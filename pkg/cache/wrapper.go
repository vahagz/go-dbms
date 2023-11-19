package cache

import (
	"encoding"
	"fmt"
	"sync"

	allocator "go-dbms/pkg/allocator/heap"

	"github.com/pkg/errors"
)

type binaryMarshalerUnmarshaler interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Dirtyable interface {
	IsDirty() bool
	Dirty(v bool)
}

type pointable interface {
	binaryMarshalerUnmarshaler
	Dirtyable
	IsNil() bool
}

type Pointable[T pointable] interface {
	RLock() *pointerWrapper[T]
	RUnlock() *pointerWrapper[T]
	Lock() *pointerWrapper[T]
	Unlock() *pointerWrapper[T]
	Get() T
	Set(val T) *pointerWrapper[T]
	Flush() *pointerWrapper[T]
	Ptr() allocator.Pointable

	binaryMarshalerUnmarshaler
}

type pointerWrapper[T pointable] struct {
	cache    *Cache[T]
	ptr      allocator.Pointable
	val      T
	accessed bool
	lock     *sync.RWMutex
}

func (p *pointerWrapper[T]) RLock() *pointerWrapper[T] {
	p.cache.lock.Lock()
	p.cache.locked[p.ptr.Addr()] = p
	p.cache.lock.Unlock()

	p.lock.RLock()
	return p
}

func (p *pointerWrapper[T]) RUnlock() *pointerWrapper[T] {
	p.cache.lock.Lock()
	delete(p.cache.locked, p.ptr.Addr())
	p.cache.lock.Unlock()

	p.lock.RUnlock()
	return p
}

func (p *pointerWrapper[T]) Lock() *pointerWrapper[T] {
	p.cache.lock.Lock()
	p.cache.locked[p.ptr.Addr()] = p
	p.cache.lock.Unlock()

	p.lock.Lock()
	return p
}

func (p *pointerWrapper[T]) Unlock() *pointerWrapper[T] {
	p.cache.lock.Lock()
	delete(p.cache.locked, p.ptr.Addr())
	p.cache.lock.Unlock()

	p.lock.Unlock()
	return p
}

func (p *pointerWrapper[T]) Get() T {
	if p.accessed {
		return p.val
	}

	itm := p.cache.newItem()
	if err := p.ptr.Get(itm); err != nil {
		panic(errors.Wrap(err, allocator.ErrUnmarshal.Error()))
	}

	p.accessed = true
	p.val = itm
	return itm
}

func (p *pointerWrapper[T]) Set(val T) *pointerWrapper[T] {
	p.accessed = true
	p.val = val
	val.Dirty(true)
	return p
}

func (p *pointerWrapper[T]) Flush() *pointerWrapper[T] {
	if p.val.IsNil() || !p.val.IsDirty() {
		return p
	}

	if err := p.ptr.Set(p.val); err != nil {
		panic(errors.Wrap(err, allocator.ErrMarshal.Error()))
	}
	p.val.Dirty(false)
	return p
}

func (p *pointerWrapper[T]) Ptr() allocator.Pointable {
	return p.ptr
}

func (p *pointerWrapper[T]) MarshalBinary() ([]byte, error) {
	return p.ptr.MarshalBinary()
}

func (p *pointerWrapper[T]) UnmarshalBinary(d []byte) error {
	return p.ptr.UnmarshalBinary(d)
}

func (p *pointerWrapper[T]) Format(f fmt.State, c rune) {
	f.Write([]byte(fmt.Sprintf("%v -> %v", p.ptr, p.val)))
}
