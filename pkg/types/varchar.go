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

const (
	DataTypeVARCHARMetaSize = 2
)

type DataTypeVARCHARMeta struct {
	Cap uint16 `json:"cap"`
}

// func (m *DataTypeVARCHARMeta) MarshalJSON() ([]byte, error) {
// 	return json.Marshal(m)
// }

// func (m *DataTypeVARCHARMeta) UnmarshalJSON(data []byte) (error) {
// 	return json.Unmarshal(data, m)
// }

func (m *DataTypeVARCHARMeta) MarshalBinary() (data []byte, err error) {
	buf := make([]byte, DataTypeVARCHARMetaSize)
	bin.PutUint16(buf, m.Cap)
	return buf, nil
}

func (m *DataTypeVARCHARMeta) UnmarshalBinary(data []byte) error {
	m.Cap = bin.Uint16(data)
	return nil
}

func (m *DataTypeVARCHARMeta) GetSize() int {
	return DataTypeVARCHARMetaSize
}

type DataTypeVARCHAR struct {
	value []byte
	Code  TypeCode             `json:"code"`
	Len   uint16               `json:"len"`
	Meta  *DataTypeVARCHARMeta `json:"meta"`
}

func (t *DataTypeVARCHAR) MarshalBinary() (data []byte, err error) {
	buf := make([]byte, t.GetSize())

	bin.PutUint16(buf[0:2], t.Len)
	copy(buf[2:], t.value)

	return buf, nil
}

func (t *DataTypeVARCHAR) UnmarshalBinary(data []byte) error {
	t.Len = bin.Uint16(data[0:2])
	copy(t.value, data[2:])
	return nil
}

func (t *DataTypeVARCHAR) Value() interface{} {
	return t.value[:t.Len]
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
	return true
}

func (t *DataTypeVARCHAR) GetSize() int {
	return 2 + int(t.Meta.Cap) // 2 is actual string length
}
