package types

import (
	"fmt"
	"go-dbms/util/helpers"
	"math"
)

func init() {
	typesMap[TYPE_INTEGER] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			m := meta.(*DataTypeINTEGERMeta)
			return &DataTypeINTEGER{
				value: make([]byte, m.ByteSize),
				Code:  m.GetCode(),
				Meta:  m,
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeINTEGERMeta{}
			}

			return &DataTypeINTEGERMeta{
				Signed:   args[0].(bool),
				ByteSize: helpers.Convert(args[1], new(uint8)),
			}
		},
	}
}

type DataTypeINTEGERMeta struct {
	Signed   bool  `json:"signed"`
	ByteSize uint8 `json:"bit_size"`
}

func (m *DataTypeINTEGERMeta) GetCode() TypeCode {
	return TYPE_INTEGER
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
			helpers.Frombytes(t.value, v)
			return *v
		} else {
			v := new(uint8)
			helpers.Frombytes(t.value, v)
			return *v
		}
	case 2:
		if t.Meta.Signed {
			v := new(int16)
			helpers.Frombytes(t.value, v)
			return *v
		} else {
			v := new(uint16)
			helpers.Frombytes(t.value, v)
			return *v
		}
	case 4:
		if t.Meta.Signed {
			v := new(int32)
			helpers.Frombytes(t.value, v)
			return *v
		} else {
			v := new(uint32)
			helpers.Frombytes(t.value, v)
			return *v
		}
	case 8:
		if t.Meta.Signed {
			v := new(int64)
			helpers.Frombytes(t.value, v)
			return *v
		} else {
			v := new(uint64)
			helpers.Frombytes(t.value, v)
			return *v
		}
	default:
		panic(fmt.Errorf("invalid byte size => %v", t.Meta.ByteSize))
	}
}

func (t *DataTypeINTEGER) Set(value interface{}) DataType {
	copy(t.value, helpers.Bytesof(value))
	return t
}

func (t *DataTypeINTEGER) Fill() DataType {
	for i := range t.value {
		t.value[i] = math.MaxUint8
	}
	return t
}

func (t *DataTypeINTEGER) Zero() DataType {
	for i := range t.value {
		t.value[i] = 0
	}
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
