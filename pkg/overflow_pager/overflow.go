package pager

import "encoding"

type OverflowMarshaler interface {
	encoding.BinaryMarshaler
	Overflows() []int
}

type OverflowUnmarshaler interface {
	encoding.BinaryUnmarshaler
	Next(data []byte) (next int, err error)
}
