package types

import (
	"encoding"
	"encoding/binary"
	"fmt"
)

type TypeCode uint8

const (
	TYPE_INT32  TypeCode = 0 // 32 bit integer
	TYPE_STRING TypeCode = 1 // variable length string
)

type DataType interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Value() interface{}
	Set(value interface{}) DataType
	GetCode() TypeCode
	GetSize() int
	IsFixedSize() bool
}

func Type(typeCode TypeCode) DataType {
	switch{
		case typeCode == TYPE_INT32:  return &DataTypeINT32{}
		case typeCode == TYPE_STRING: return &DataTypeSTRING{}
		default: panic(fmt.Errorf("invalid typeCode => %v", typeCode))
	}
}

var bin = binary.BigEndian
