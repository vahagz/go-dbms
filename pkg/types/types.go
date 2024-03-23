package types

import (
	"encoding"
	"encoding/json"
	"errors"
)

type TypeCode uint8

const (
	TYPE_INTEGER TypeCode = iota // 8/16/32/64 bit [un]signed integer
	TYPE_STRING                  // variable length string
	TYPE_VARCHAR                 // fixed length string
	TYPE_FLOAT                   // 32/64 bit floating point number
)

type Operator string

const (
	Equal          Operator = "="
	GreaterOrEqual Operator = ">="
	LessOrEqual    Operator = "<="
	Greater        Operator = ">"
	Less           Operator = "<"
	NotEqual       Operator = "!="
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

	Copy() DataType
	MetaCopy() DataTypeMeta
	Bytes() []byte
	Value() json.Token
	Set(value interface{}) DataType
	Fill() DataType
	Zero() DataType
	CompareOp(operator Operator, val DataType) bool
	Compare(val DataType) int
	Cast(meta DataTypeMeta) (DataType, error)
}

type DataRow map[string]DataType

func (dr DataRow) Compare(dr2 DataRow, keys []string) int {
	for _, col := range keys {
		cmpVal := dr[col].Compare(dr2[col])
		switch cmpVal {
			case -1, 1: return cmpVal
		}
	}
	return 0
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
			if float64(int(v)) == v {
				return Type(Meta(TYPE_INTEGER, true, 8, false)).Set(int(v)) // v is int
			}
			return Type(Meta(TYPE_FLOAT, 8)).Set(v) // v is float
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
