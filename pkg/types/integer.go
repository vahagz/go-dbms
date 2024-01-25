package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"go-dbms/util/helpers"
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
				AI:       autoIncrement{ Enabled: args[2].(bool) },
			}
		},
	}
}

type autoIncrement struct {
	Enabled bool   `json:"enabled"`
	Value   uint64 `json:"value"`
}

type DataTypeINTEGERMeta struct {
	Signed   bool          `json:"signed"`
	ByteSize uint8         `json:"bit_size"`
	AI       autoIncrement `json:"auto_increment,omitempty"`
}

func (m *DataTypeINTEGERMeta) GetCode() TypeCode {
	return TYPE_INTEGER
}

func (m *DataTypeINTEGERMeta) Size() int {
	return int(m.ByteSize)
}

func (m *DataTypeINTEGERMeta) Default() DataType {
	cp := *m
	if m.AI.Enabled {
		m.AI.Value++
		return Type(&cp).Set(m.AI.Value)
	}
	return Type(&cp).Set(0)
}

func (m *DataTypeINTEGERMeta) IsFixedSize() bool {
	return true
}

func (m *DataTypeINTEGERMeta) IsNumeric() bool {
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
	cp := append(make([]byte, 0, 8), t.value...)
	if len(cp) < 8 {
		for i := len(cp); i < 8; i++ {
			cp = append(cp, 0)
		}
	}

	b := make([]byte, len(cp))
	binary.LittleEndian.PutUint64(b, binary.BigEndian.Uint64(cp))
	return b[len(b)-len(t.value):]
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

func (t *DataTypeINTEGER) Default() DataType {
	return t.Meta.Default()
}

func (t *DataTypeINTEGER) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeINTEGER) IsNumeric() bool {
	return t.Meta.IsNumeric()
}

func (t *DataTypeINTEGER) Size() int {
	return int(t.Meta.ByteSize)
}

func (t *DataTypeINTEGER) Compare(operator string, val DataType) bool {
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

func (t *DataTypeINTEGER) Cast(code TypeCode, meta DataTypeMeta) (DataType, error) {
	switch code {
		case TYPE_INTEGER: {
			if meta == nil {
				meta = t.Meta
			}
			return Type(meta).Set(t.Value()), nil
		}
		case TYPE_STRING, TYPE_VARCHAR: {
			if meta == nil {
				meta = t.Meta
			} else if code == TYPE_VARCHAR {
				meta = &DataTypeVARCHARMeta{
					Cap: uint16(t.Meta.ByteSize),
				}
			}
			return Type(meta).Set(fmt.Sprint(t.Value())), nil
		}
	}

	return nil, fmt.Errorf("typecast from %v to %v not supported", t.Code, code)
}
