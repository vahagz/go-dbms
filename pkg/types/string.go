package types

import (
	"bytes"
	"errors"
	"fmt"
)

func init() {
	typesMap[TYPE_STRING] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			return &DataTypeSTRING{
				Code: meta.GetCode(),
				Meta: meta.(*DataTypeSTRINGMeta),
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeSTRINGMeta{}
			}

			return &DataTypeSTRINGMeta{}
		},
	}
}

type DataTypeSTRINGMeta struct {
}

func (m *DataTypeSTRINGMeta) GetCode() TypeCode {
	return TYPE_STRING
}

func (m *DataTypeSTRINGMeta) Size() int {
	return -1
}

func (m *DataTypeSTRINGMeta) Default() DataType {
	cp := *m
	return Type(&cp).Set("")
}

func (m *DataTypeSTRINGMeta) IsFixedSize() bool {
	return false
}

func (m *DataTypeSTRINGMeta) IsNumeric() bool {
	return false
}

type DataTypeSTRING struct {
	value string
	Code  TypeCode            `json:"code"`
	Meta  *DataTypeSTRINGMeta `json:"meta"`
}

func (t *DataTypeSTRING) MarshalBinary() (data []byte, err error) {
	return []byte(t.value), nil
}

func (t *DataTypeSTRING) UnmarshalBinary(data []byte) error {
	t.value = string(data)
	return nil
}

func (t *DataTypeSTRING) Bytes() []byte {
	return []byte(t.value)
}

func (t *DataTypeSTRING) Value() interface{} {
	return t.value
}

func (t *DataTypeSTRING) Set(value interface{}) DataType {
	v, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("invalid set data type => %v", value))
	}

	t.value = v
	return t
}

func (t *DataTypeSTRING) Fill() DataType {
	panic(errors.New("Fill not allowed for string type"))
}

func (t *DataTypeSTRING) Zero() DataType {
	panic(errors.New("Zero not allowed for string type"))
}

func (t *DataTypeSTRING) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeSTRING) Default() DataType {
	return t.Meta.Default()
}

func (t *DataTypeSTRING) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeSTRING) IsNumeric() bool {
	return t.Meta.IsNumeric()
}

func (t *DataTypeSTRING) Size() int {
	return len(t.value)
}

func (t *DataTypeSTRING) Compare(operator string, val DataType) bool {
	switch operator {
		case "=": return bytes.Compare(t.Bytes(), val.Bytes()) == 0
		case ">=": return bytes.Compare(t.Bytes(), val.Bytes()) >= 0
		case "<=": return bytes.Compare(t.Bytes(), val.Bytes()) <= 0
		case ">": return bytes.Compare(t.Bytes(), val.Bytes()) > 0
		case "<": return bytes.Compare(t.Bytes(), val.Bytes()) < 0
		case "!=": return bytes.Compare(t.Bytes(), val.Bytes()) != 0
	}
	panic(fmt.Errorf("invalid operator:'%s'", operator))
}
