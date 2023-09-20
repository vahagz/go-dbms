package types

import "golang.org/x/exp/constraints"

type DataTypeINTEGER[T constraints.Integer] struct {
	value T
}

func (t *DataTypeINTEGER[T]) MarshalBinary() (data []byte, err error) {
	return bytesof(t.value), nil
}

func (t *DataTypeINTEGER[T]) UnmarshalBinary(data []byte) error {
	frombytes(data, &t.value)
	return nil
}

func (t *DataTypeINTEGER[T]) Value() interface{} {
	return t.value
}

func (t *DataTypeINTEGER[T]) Set(value interface{}) DataType {
	convert(value, &t.value)
	return t
}

func (t *DataTypeINTEGER[T]) GetCode() TypeCode {
	return TYPE_INT32
}

func (t *DataTypeINTEGER[T]) IsFixedSize() bool {
	return true
}

func (t *DataTypeINTEGER[T]) GetSize() int {
	return 4
}
