package cache

import (
	"encoding"
	"fmt"

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
	Get() U
	Set(val U)
	Addr() uint64
	binaryMarshalerUnmarshaler
}

type pointerWrapper[T any, U bmu[T]] struct {
	cache *Cache[T, U]
	ptr   allocator.Pointable
	val   U
}

func (p *pointerWrapper[T, U]) Get() U {
	var t T
	itm := U(&t)
	if err := p.ptr.Get(itm); err != nil {
		panic(errors.Wrap(err, allocator.ErrMarshal.Error()))
	}
	return itm
}

func (p *pointerWrapper[T, U]) Set(itm U) {
	if err := p.ptr.Set(itm); err != nil {
		panic(errors.Wrap(err, allocator.ErrMarshal.Error()))
	}
}

func (p *pointerWrapper[T, U]) Addr() uint64 {
	return p.ptr.Addr()
}

func (p *pointerWrapper[T, U]) MarshalBinary() ([]byte, error) {
	return p.ptr.MarshalBinary()
}

func (p *pointerWrapper[T, U]) UnmarshalBinary(d []byte) error {
	return p.ptr.UnmarshalBinary(d)
}

func (p *pointerWrapper[T, U]) Format(f fmt.State, c rune) {
	f.Write([]byte(fmt.Sprint(p.ptr)))
}
