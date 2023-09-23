package types

import (
	"fmt"
)

func NewINTEGERMeta(signed bool, byteSize uint8) *DataTypeINTEGERMeta {
	return &DataTypeINTEGERMeta{
		Signed:   signed,
		ByteSize: byteSize,
	}
}

func NewINTEGER(code TypeCode, meta *DataTypeINTEGERMeta) *DataTypeINTEGER {
	return &DataTypeINTEGER{
		value: make([]byte, meta.ByteSize),
		Code:  code,
		Meta:  meta,
	}
}

type DataTypeINTEGERMeta struct {
	Signed   bool  `json:"signed"`
	ByteSize uint8 `json:"bit_size"`
}

func (m *DataTypeINTEGERMeta) GetCode() TypeCode {
	return TYPE_INT
}

func (m *DataTypeINTEGERMeta) Size() int {
	return int(m.ByteSize)
}

func (m *DataTypeINTEGERMeta) IsFixedSize() bool {
	return true
}

type DataTypeINTEGER struct {
	value []byte
	Code  TypeCode             `json:"code"`
	Meta  *DataTypeINTEGERMeta `json:"meta"`
}

func (t *DataTypeINTEGER) MarshalBinary() (data []byte, err error) {
	return t.value, nil
}

func (t *DataTypeINTEGER) UnmarshalBinary(data []byte) error {
	copy(t.value, data)
	return nil
}

func (t *DataTypeINTEGER) Bytes() []byte {
	return t.value
}

func (t *DataTypeINTEGER) Value() interface{} {
	switch t.Meta.ByteSize {
	case 1:
		if t.Meta.Signed {
			v := new(int8)
			frombytes(t.value, v)
			return *v
		} else {
			v := new(uint8)
			frombytes(t.value, v)
			return *v
		}
	case 2:
		if t.Meta.Signed {
			v := new(int16)
			frombytes(t.value, v)
			return *v
		} else {
			v := new(uint16)
			frombytes(t.value, v)
			return *v
		}
	case 4:
		if t.Meta.Signed {
			v := new(int32)
			frombytes(t.value, v)
			return *v
		} else {
			v := new(uint32)
			frombytes(t.value, v)
			return *v
		}
	case 8:
		if t.Meta.Signed {
			v := new(int64)
			frombytes(t.value, v)
			return *v
		} else {
			v := new(uint64)
			frombytes(t.value, v)
			return *v
		}
	default:
		panic(fmt.Errorf("invalid byte size => %v", t.Meta.ByteSize))
	}
}

func (t *DataTypeINTEGER) Set(value interface{}) DataType {
	copy(t.value, bytesof(value))
	return t
}

func (t *DataTypeINTEGER) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeINTEGER) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeINTEGER) Size() int {
	return int(t.Meta.ByteSize)
}
