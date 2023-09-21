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

const (
	DataTypeSTRINGMetaSize = 0
)

type DataTypeSTRINGMeta struct {
}

// func (m *DataTypeSTRINGMeta) MarshalJSON() ([]byte, error) {
// 	return json.Marshal(m)
// }

// func (m *DataTypeSTRINGMeta) UnmarshalJSON(data []byte) (error) {
// 	return json.Unmarshal(data, m)
// }

func (m *DataTypeSTRINGMeta) MarshalBinary() (data []byte, err error) {
	return make([]byte, 0), nil
}

func (m *DataTypeSTRINGMeta) UnmarshalBinary(data []byte) error {
	return nil
}

func (m *DataTypeSTRINGMeta) GetSize() int {
	return DataTypeSTRINGMetaSize
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
	return false
}

func (t *DataTypeSTRING) GetSize() int {
	return len(t.value)
}
