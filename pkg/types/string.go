package types

import (
	"fmt"
)

func NewSTRINGMeta() *DataTypeSTRINGMeta {
	return &DataTypeSTRINGMeta{}
}

func NewSTRING(code TypeCode, meta *DataTypeSTRINGMeta) *DataTypeSTRING {
	return &DataTypeSTRING{
		Code: code,
		Meta: meta,
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

func (m *DataTypeSTRINGMeta) IsFixedSize() bool {
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

func (t *DataTypeSTRING) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeSTRING) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeSTRING) Size() int {
	return len(t.value)
}
