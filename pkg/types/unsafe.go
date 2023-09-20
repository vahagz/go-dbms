package types

import (
	"reflect"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type eface struct {
	typ, val unsafe.Pointer
}

func sizeof[T any](v T) int {
	return int(reflect.TypeOf(v).Size())
}

func bytesof(v interface{}) []byte {
	return unsafe.Slice((*byte)((*eface)(unsafe.Pointer(&v)).val), sizeof(v))
}

func frombytes[T any](srcBytes []byte, dst *T) {
	dstBytes := make([]byte, sizeof(*dst))
	copy(dstBytes, srcBytes)
	*dst = *(*T)(unsafe.Pointer(&dstBytes[0]))
}

func convert[T constraints.Integer](from interface{}, to *T) {
	srcSize := sizeof(from)
	dstSize := sizeof(*to)

	if srcSize >= dstSize {
		*to = *(*T)((*eface)(unsafe.Pointer(&from)).val)
		return
	}

	frombytes(bytesof(from), to)
}
