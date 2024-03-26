package types

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var ErrInvalidDataType = fmt.Errorf("invalid set data type")

type TypeCode uint8

const (
	TYPE_INTEGER TypeCode = iota // 8/16/32/64 bit [un]signed integer
	TYPE_STRING                  // variable length string
	TYPE_VARCHAR                 // fixed length string
	TYPE_FLOAT                   // 32/64 bit floating point number
	TYPE_DATETIME                // "2024-03-25 17:15:06" format datetime
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

type DataTypeMeta interface {
	GetCode() TypeCode
	Size() int
	Default() DataType
	IsFixedSize() bool
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

var supportedIntSizes = []string{"8","16","32","64"}
var supportedFloatSizes = []string{"32","64"}
var intRegex, _ = regexp.Compile(fmt.Sprintf("^(^|U)Int(%s)$", strings.Join(supportedIntSizes, "|")))
var floatRegex, _ = regexp.Compile(fmt.Sprintf("^Float(%s)$", strings.Join(supportedFloatSizes, "|")))

var parsers = map[string]func(tokens []string)DataTypeMeta{}

func Parse(tokens []string) DataTypeMeta {
	typeName := tokens[0]

	if ok := intRegex.Match([]byte(typeName)); ok {
		return parsers["INT"](tokens)
	} else if ok := floatRegex.Match([]byte(typeName)); ok {
		return parsers["FLOAT"](tokens)
	}
	return parsers[typeName](tokens)
}
