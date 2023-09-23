package types

import (
	"fmt"
)

func NewVARCHARMeta(length uint16) *DataTypeVARCHARMeta {
	return &DataTypeVARCHARMeta{
		Cap: length,
	}
}

func NewVARCHAR(code TypeCode, meta *DataTypeVARCHARMeta) *DataTypeVARCHAR {
	return &DataTypeVARCHAR{
		value: make([]byte, meta.Cap),
		Code:  code,
		Meta:  meta,
	}
}

type DataTypeVARCHARMeta struct {
	Cap uint16 `json:"cap"`
}

func (m *DataTypeVARCHARMeta) GetCode() TypeCode {
	return TYPE_VARCHAR
}

func (m *DataTypeVARCHARMeta) Size() int {
	return int(m.Cap)
}

func (m *DataTypeVARCHARMeta) IsFixedSize() bool {
	return true
}

type DataTypeVARCHAR struct {
	value []byte
	Code  TypeCode             `json:"code"`
	Len   uint16               `json:"len"`
	Meta  *DataTypeVARCHARMeta `json:"meta"`
}

func (t *DataTypeVARCHAR) MarshalBinary() (data []byte, err error) {
	return t.value, nil
}

func (t *DataTypeVARCHAR) UnmarshalBinary(data []byte) error {
	t.Len = uint16(copy(t.value, data))
	return nil
}

func (t *DataTypeVARCHAR) Bytes() []byte {
	return t.value[:t.Len]
}

func (t *DataTypeVARCHAR) Value() interface{} {
	return string(t.value[:t.Len])
}

func (t *DataTypeVARCHAR) Set(value interface{}) DataType {
	switch value.(type) {
	case []byte: t.Len = uint16(copy(t.value, value.([]byte)))
	case string: t.Len = uint16(copy(t.value, []byte(value.(string))))
	default:     panic(fmt.Errorf("invalid set data type => %v", value))
	}

	return t
}

func (t *DataTypeVARCHAR) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeVARCHAR) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeVARCHAR) Size() int {
	return int(t.Meta.Cap)
}
