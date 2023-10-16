package allocator

import (
	"fmt"

	"github.com/pkg/errors"
)

func Wrap[T any, U bmu[T]](ptr Pointable) WrappedPointable[T, U] {
	return &pointerWrapper[T, U]{ptr}
}

type bmu[T any] interface {
	*T
	binaryMarshalerUnmarshaler
}

type WrappedPointable[T any, U bmu[T]] interface {
	Get() U
	Set(val U)
	Addr() uint64
	binaryMarshalerUnmarshaler
}

type pointerWrapper[T any, U bmu[T]] struct {
	ptr Pointable
}

func (p *pointerWrapper[T, U]) Get() U {
	var t T
	itm := U(&t)
	if err := p.ptr.Get(itm); err != nil {
		panic(errors.Wrap(err, ErrMarshal.Error()))
	}
	return itm
}

func (p *pointerWrapper[T, U]) Set(itm U) {
	if err := p.ptr.Set(itm); err != nil {
		panic(errors.Wrap(err, ErrMarshal.Error()))
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
