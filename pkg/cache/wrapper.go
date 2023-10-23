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

type bmu[T any] interface {
	*T
	binaryMarshalerUnmarshaler
	Dirtyable
}

type Pointable[T any, U bmu[T]] interface {
	RLock() *pointerWrapper[T, U]
	RUnlock() *pointerWrapper[T, U]
	Lock() *pointerWrapper[T, U]
	Unlock() *pointerWrapper[T, U]
	Get() U
	Set(val U) *pointerWrapper[T, U]
	Flush() *pointerWrapper[T, U]

	binaryMarshalerUnmarshaler
}

type pointerWrapper[T any, U bmu[T]] struct {
	cache    *Cache[T, U]
	ptr      allocator.Pointable
	val      U
	accessed bool
	lock     *sync.RWMutex
}

func (p *pointerWrapper[T, U]) RLock() *pointerWrapper[T, U] {
	p.cache.lock.Lock()
	p.cache.locked[p.ptr.Addr()] = p
	p.cache.lock.Unlock()

	p.lock.RLock()
	return p
}

func (p *pointerWrapper[T, U]) RUnlock() *pointerWrapper[T, U] {
	p.cache.lock.Lock()
	delete(p.cache.locked, p.ptr.Addr())
	p.cache.lock.Unlock()

	p.lock.RUnlock()
	return p
}

func (p *pointerWrapper[T, U]) Lock() *pointerWrapper[T, U] {
	p.cache.lock.Lock()
	p.cache.locked[p.ptr.Addr()] = p
	p.cache.lock.Unlock()

	p.lock.Lock()
	return p
}

func (p *pointerWrapper[T, U]) Unlock() *pointerWrapper[T, U] {
	p.cache.lock.Lock()
	delete(p.cache.locked, p.ptr.Addr())
	p.cache.lock.Unlock()

	p.lock.Unlock()
	return p
}

func (p *pointerWrapper[T, U]) Get() U {
	if p.accessed {
		return p.val
	}

	var t T
	itm := U(&t)
	if err := p.ptr.Get(itm); err != nil {
		panic(errors.Wrap(err, allocator.ErrUnmarshal.Error()))
	}

	p.accessed = true
	p.val = itm
	return itm
}

func (p *pointerWrapper[T, U]) Set(val U) *pointerWrapper[T, U] {
	p.accessed = true
	p.val = val
	val.Dirty(true)
	return p
}

func (p *pointerWrapper[T, U]) Flush() *pointerWrapper[T, U] {
	if err := p.ptr.Set(p.val); err != nil {
		panic(errors.Wrap(err, allocator.ErrMarshal.Error()))
	}
	return p
}

func (p *pointerWrapper[T, U]) MarshalBinary() ([]byte, error) {
	return p.ptr.MarshalBinary()
}

func (p *pointerWrapper[T, U]) UnmarshalBinary(d []byte) error {
	return p.ptr.UnmarshalBinary(d)
}

func (p *pointerWrapper[T, U]) Format(f fmt.State, c rune) {
	f.Write([]byte(fmt.Sprintf("%v -> %v", p.ptr, p.val)))
}
