package helpers

import (
	"reflect"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type eface struct {
	typ, val unsafe.Pointer
}

func Sizeof[T any](v T) int {
	return int(reflect.TypeOf(v).Size())
}

func Bytesof(v interface{}) []byte {
	return unsafe.Slice((*byte)((*eface)(unsafe.Pointer(&v)).val), Sizeof(v))
}

func Frombytes[T any](srcBytes []byte, dst *T) {
	dstBytes := make([]byte, Sizeof(*dst))
	copy(dstBytes, srcBytes)
	*dst = *(*T)(unsafe.Pointer(&dstBytes[0]))
}

func Convert[T constraints.Integer](from interface{}, to *T) T {
	srcSize := Sizeof(from)
	dstSize := Sizeof(*to)

	if srcSize >= dstSize {
		*to = *(*T)((*eface)(unsafe.Pointer(&from)).val)
		return *to
	}

	Frombytes(Bytesof(from), to)
	return *to
}
