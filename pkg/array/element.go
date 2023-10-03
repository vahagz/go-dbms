package array

import (
	"encoding"
	"errors"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type elementer[T any] interface {
	Size() uint16
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	*T
}

var ErrInvalidBitSize = errors.New("invalid bit size")

func NewNumber[T constraints.Integer](n T) *Number[T] {
	return &Number[T]{
		num: n,
	}
}

type Number[T constraints.Integer] struct {
	num T
}

func (n *Number[T]) Value() T {
	return n.num
}

func (n *Number[T]) Size() uint16 {
	return uint16(unsafe.Sizeof(n.num))
}

func (n *Number[T]) MarshalBinary() ([]byte, error) {
	bitSize := n.Size()
	buf := make([]byte, bitSize)
	if bitSize == 1 {
		buf[0] = byte(n.num)
	} else if bitSize == 2 {
		bin.PutUint16(buf, uint16(n.num))
	} else if bitSize == 4 {
		bin.PutUint32(buf, uint32(n.num))
	} else if bitSize == 8 {
		bin.PutUint64(buf, uint64(n.num))
	} else {
		panic(ErrInvalidBitSize)
	}
	return buf, nil
}

func (n *Number[T]) UnmarshalBinary(d []byte) error {
	bitSize := n.Size()
	if bitSize == 1 {
		n.num = T(d[0])
	} else if bitSize == 2 {
		n.num = T(bin.Uint16(d))
	} else if bitSize == 4 {
		n.num = T(bin.Uint32(d))
	} else if bitSize == 8 {
		n.num = T(bin.Uint64(d))
	} else {
		panic(ErrInvalidBitSize)
	}
	return nil
}
