package types

import (
	"errors"
)

var errUnimplemented = errors.New("unimplemented")

type DataTypeBASE[T DataTypeMeta] struct {
	Code  TypeCode `json:"code"`
	Meta  T        `json:"meta"`
}

func (t *DataTypeBASE[T]) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeBASE[T]) Default() DataType {
	return t.Meta.Default()
}

func (t *DataTypeBASE[T]) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeBASE[T]) Size() int {
	return t.Meta.Size()
}
