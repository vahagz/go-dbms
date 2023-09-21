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

const (
	DataTypeINTEGERMetaSize = 1
  signBit                 = 0b00010000
	sizeBit                 = 0b00001111
)

type DataTypeINTEGERMeta struct {
	Signed   bool  `json:"signed"`
	ByteSize uint8 `json:"bit_size"`
}

// func (m *DataTypeINTEGERMeta) MarshalJSON() ([]byte, error) {
// 	return json.Marshal(m)
// }

// func (m *DataTypeINTEGERMeta) UnmarshalJSON(data []byte) (error) {
// 	return json.Unmarshal(data, m)
// }

// func (m *DataTypeINTEGERMeta) MarshalBinary() (data []byte, err error) {
// 	buf := make([]byte, DataTypeINTEGERMetaSize)
// 	buf[0] = m.ByteSize
// 	if m.Signed {
// 		buf[0] = buf[0] | signBit
// 	}
// 	return buf, nil
// }

// func (m *DataTypeINTEGERMeta) UnmarshalBinary(data []byte) error {
// 	m.Signed = data[0] & signBit == 1
// 	m.ByteSize = data[0] & sizeBit
// 	return nil
// }

func (m *DataTypeINTEGERMeta) GetSize() int {
	return DataTypeINTEGERMetaSize
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
	return true
}

func (t *DataTypeINTEGER) GetSize() int {
	return int(t.Meta.ByteSize)
}
