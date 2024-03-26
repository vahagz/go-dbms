package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"go-dbms/util/helpers"
)

func init() {
	typesMap[TYPE_INTEGER] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			m := meta.(*DataTypeINTEGERMeta)
			return &DataTypeINTEGER{
				value: make([]byte, m.ByteSize),
				DataTypeBASE: DataTypeBASE[*DataTypeINTEGERMeta]{
					Code: m.GetCode(),
					Meta: m,
				},
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeINTEGERMeta{}
			}

			return &DataTypeINTEGERMeta{
				Signed:   args[0].(bool),
				ByteSize: helpers.Convert[uint8](args[1]),
				AI:       autoIncrement{ Enabled: args[2].(bool) },
			}
		},
	}

	parsers["INT"] = func(tokens []string) DataTypeMeta {
		typeName := tokens[0]
		var (
			err error
			signed, ai bool
			size int
		)

		parts := strings.Split(typeName, "Int")
		signed = parts[0] == "U"
		size, err = strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			panic(err)
		}

		if len(tokens) > 1 {
			ai = tokens[1] == "AUTO" && tokens[2] == "INCREMENT"
		}

		return Meta(TYPE_INTEGER, signed, size/8, ai)
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


type DataTypeINTEGER struct {
	value []byte
	DataTypeBASE[*DataTypeINTEGERMeta]
}

func (t *DataTypeINTEGER) MarshalBinary() (data []byte, err error) {
	return t.value, nil
}

func (t *DataTypeINTEGER) UnmarshalBinary(data []byte) error {
	copy(t.value, data)
	return nil
}

func (t *DataTypeINTEGER) Copy() DataType {
	return &DataTypeINTEGER{
		value: slices.Clone(t.value),
		DataTypeBASE: DataTypeBASE[*DataTypeINTEGERMeta]{
			Code: t.GetCode(),
			Meta: t.MetaCopy().(*DataTypeINTEGERMeta),
		},
	}
}

func (t *DataTypeINTEGER) MetaCopy() DataTypeMeta {
	return &DataTypeINTEGERMeta{
		Signed:   t.Meta.Signed,
		ByteSize: t.Meta.ByteSize,
		AI:       t.Meta.AI,
	}
}

func (t *DataTypeINTEGER) Bytes() []byte {
	cp := slices.Clone(t.value)
	slices.Reverse(cp)
	return cp
}

func (t *DataTypeINTEGER) Value() json.Token {
	switch size := t.Meta.ByteSize; {
		case t.Meta.Signed  && size == 1: return helpers.Frombytes[int8](t.value)
		case t.Meta.Signed  && size == 2: return helpers.Frombytes[int16](t.value)
		case t.Meta.Signed  && size == 4: return helpers.Frombytes[int32](t.value)
		case t.Meta.Signed  && size == 8: return helpers.Frombytes[int64](t.value)
		case !t.Meta.Signed && size == 1: return helpers.Frombytes[uint8](t.value)
		case !t.Meta.Signed && size == 2: return helpers.Frombytes[uint16](t.value)
		case !t.Meta.Signed && size == 4: return helpers.Frombytes[uint32](t.value)
		case !t.Meta.Signed && size == 8: return helpers.Frombytes[uint64](t.value)
		default: panic(fmt.Errorf("invalid byte size => %v", t.Meta.ByteSize))
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

func (t *DataTypeINTEGER) Size() int {
	return t.Meta.Size()
}

func (t *DataTypeINTEGER) Compare(val DataType) int {
	return bytes.Compare(t.Bytes(), val.Bytes())
}

func (t *DataTypeINTEGER) CompareOp(operator Operator, val DataType) bool {
	switch operator {
		case Equal:          return t.Compare(val) == 0
		case GreaterOrEqual: return t.Compare(val) >= 0
		case LessOrEqual:    return t.Compare(val) <= 0
		case Greater:        return t.Compare(val) > 0
		case Less:           return t.Compare(val) < 0
		case NotEqual:       return t.Compare(val) != 0
	}
	panic(fmt.Errorf("invalid operator:'%s'", operator))
}

func (t *DataTypeINTEGER) Cast(meta DataTypeMeta) (DataType, error) {
	code := meta.GetCode()
	switch code {
		case TYPE_INTEGER: {
			if meta == nil {
				meta = t.Meta
			}
			return Type(meta).Set(t.Value()), nil
		}
		case TYPE_FLOAT: {
			if meta == nil {
				meta = float64Meta
			}
			return Type(meta).Set(float64(helpers.Convert[int64](t.Value()))), nil
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
		case TYPE_DATETIME: {
			if meta == nil {
				meta = &DataTypeDATETIMEMeta{}
			}
			return Type(meta).Set(helpers.Frombytes[int64](t.value)), nil
		}
	}

	return nil, fmt.Errorf("typecast from %v to %v not supported", t.Code, code)
}
