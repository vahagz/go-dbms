package types

import (
	"encoding"
	"encoding/binary"
	"fmt"
)

type TypeCode uint8

const (
	TYPE_INT     TypeCode = 0 // 32 bit integer
	TYPE_STRING  TypeCode = 1 // variable length string
	TYPE_VARCHAR TypeCode = 2 // fixed length string
)

var bin = binary.BigEndian

type DataTypeMeta interface {
	GetCode() TypeCode
	Size() int
	IsFixedSize() bool
}

type DataType interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	DataTypeMeta

	Bytes() []byte

	Value() interface{}
	Set(value interface{}) DataType
}

func Type(meta DataTypeMeta) DataType {
	typeCode := meta.GetCode()
	switch{
		case typeCode == TYPE_INT:     return NewINTEGER(typeCode, meta.(*DataTypeINTEGERMeta))
		case typeCode == TYPE_STRING:  return NewSTRING(typeCode, meta.(*DataTypeSTRINGMeta))
		case typeCode == TYPE_VARCHAR: return NewVARCHAR(typeCode, meta.(*DataTypeVARCHARMeta))
		default:                       panic(fmt.Errorf("[Type] invalid typeCode => %v", typeCode))
	}
}

func Meta(typeCode TypeCode, empty bool, args ...interface{}) DataTypeMeta {
	switch{
		case typeCode == TYPE_INT:
			if empty { return &DataTypeINTEGERMeta{} }
			return NewINTEGERMeta(args[0].(bool), convert(args[1], new(uint8)))
		case typeCode == TYPE_STRING:
			if empty { return &DataTypeSTRINGMeta{} }
			return NewSTRINGMeta()
		case typeCode == TYPE_VARCHAR:
			if empty { return &DataTypeVARCHARMeta{} }
			return NewVARCHARMeta(convert(args[0], new(uint16)))
		default:
			panic(fmt.Errorf("[Meta] invalid typeCode => %v", typeCode))
	}
}
