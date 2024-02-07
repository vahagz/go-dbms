package types

import (
	"encoding"
	"errors"
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
var numericTypes = map[TypeCode]struct{}{}

type DataTypeMeta interface {
	GetCode() TypeCode
	Size() int
	Default() DataType
	IsFixedSize() bool
	IsNumeric() bool
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
	Compare(operator string, val DataType) bool
	Cast(code TypeCode, meta DataTypeMeta) (DataType, error)
}

func Type(meta DataTypeMeta) DataType {
	return typesMap[meta.GetCode()].newInstance(meta)
}

func Meta(typeCode TypeCode, args ...interface{}) DataTypeMeta {
	return typesMap[typeCode].newMeta(args...)
}

func ParseJSONValue(item interface{}) DataType {
	switch v := item.(type) {
		case float64: {
			if float64(int(v)) == v { // v is int
				return Type(Meta(TYPE_INTEGER, true, 8, false)).Set(int(v))
			} else { // v is float
				// TODO: dt = Type(Meta(TYPE_FLOAT64)).Set(v)
				return Type(Meta(TYPE_INTEGER, true, 8, false)).Set(int(v))
			}
		}
		case string: {
			return Type(Meta(TYPE_STRING)).Set(v)
		}
		default: {
			panic(errors.New("invalid item type"))
		}
	}
}

func IsNumeric(code TypeCode) bool {
	_, ok := numericTypes[code]
	return ok
}
