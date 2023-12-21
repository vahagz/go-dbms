package types

import (
	"encoding"
)

type TypeCode uint8

const (
	TYPE_INTEGER TypeCode = iota // 32 bit integer
	TYPE_STRING                  // variable length string
	TYPE_VARCHAR                 // fixed length string
)

type newable struct {
	newInstance func(meta DataTypeMeta) DataType
	newMeta     func(args ...interface{}) DataTypeMeta
}

var typesMap = map[TypeCode]newable{}

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
	Fill() DataType
	Zero() DataType
}

func Type(meta DataTypeMeta) DataType {
	return typesMap[meta.GetCode()].newInstance(meta)
}

func Meta(typeCode TypeCode, args ...interface{}) DataTypeMeta {
	return typesMap[typeCode].newMeta(args...)
}
