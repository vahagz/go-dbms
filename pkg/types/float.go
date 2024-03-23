package types

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

var float64Meta = &DataTypeFLOATMeta{ByteSize: 8}

func init() {
	numericTypes[TYPE_FLOAT] = struct{}{}

	typesMap[TYPE_FLOAT] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			m := meta.(*DataTypeFLOATMeta)
			return &DataTypeFLOAT{
				value: make([]byte, m.ByteSize),
				Code:  m.GetCode(),
				Meta:  m,
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeFLOATMeta{}
			}

			return &DataTypeFLOATMeta{
				ByteSize: helpers.Convert[uint8](args[0]),
			}
		},
	}
}

type DataTypeFLOATMeta struct {
	ByteSize uint8 `json:"bit_size"`
}

func (m *DataTypeFLOATMeta) GetCode() TypeCode {
	return TYPE_FLOAT
}

func (m *DataTypeFLOATMeta) Size() int {
	return int(m.ByteSize)
}

func (m *DataTypeFLOATMeta) Default() DataType {
	return Type(&DataTypeFLOATMeta{
		ByteSize: m.ByteSize,
	}).Set(0.0)
}

func (m *DataTypeFLOATMeta) IsFixedSize() bool {
	return true
}

func (m *DataTypeFLOATMeta) IsNumeric() bool {
	return true
}


type DataTypeFLOAT struct {
	value []byte
	Code  TypeCode           `json:"code"`
	Meta  *DataTypeFLOATMeta `json:"meta"`
}

func (t *DataTypeFLOAT) MarshalBinary() (data []byte, err error) {
	return t.value, nil
}

func (t *DataTypeFLOAT) UnmarshalBinary(data []byte) error {
	copy(t.value, data)
	return nil
}

func (t *DataTypeFLOAT) Copy() DataType {
	return &DataTypeFLOAT{
		value: slices.Clone(t.value),
		Code:  t.Code,
		Meta:  t.MetaCopy().(*DataTypeFLOATMeta),
	}
}

func (t *DataTypeFLOAT) MetaCopy() DataTypeMeta {
	return &DataTypeFLOATMeta{
		ByteSize: t.Meta.ByteSize,
	}
}

func (t *DataTypeFLOAT) Bytes() []byte {
	return slices.Clone(t.value)
}

func (t *DataTypeFLOAT) Value() json.Token {
	switch t.Meta.ByteSize {
		case 4: return math.Float32frombits(helpers.Frombytes[uint32](t.value))
		case 8: return math.Float64frombits(helpers.Frombytes[uint64](t.value))
		default: panic(fmt.Errorf("invalid byte size => %v", t.Meta.ByteSize))
	}
}

func (t *DataTypeFLOAT) Set(value interface{}) DataType {
	switch value.(type) {
		case float32: {
			if t.Meta.ByteSize == 4 {
				t.value = helpers.Bytesof(math.Float32bits(value.(float32)))
			} else if t.Meta.ByteSize == 8 {
				t.value = helpers.Bytesof(math.Float64bits(float64(value.(float32))))
			}
		}
		case float64: {
			if t.Meta.ByteSize == 4 {
				t.value = helpers.Bytesof(math.Float32bits(float32(value.(float64))))
			} else if t.Meta.ByteSize == 8 {
				t.value = helpers.Bytesof(math.Float64bits(value.(float64)))
			}
		}
	}
	return t
}

func (t *DataTypeFLOAT) Fill() DataType {
	for i := range t.value {
		t.value[i] = math.MaxUint8
	}
	return t
}

func (t *DataTypeFLOAT) Zero() DataType {
	for i := range t.value {
		t.value[i] = 0
	}
	return t
}

func (t *DataTypeFLOAT) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeFLOAT) Default() DataType {
	return t.Meta.Default()
}

func (t *DataTypeFLOAT) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeFLOAT) IsNumeric() bool {
	return t.Meta.IsNumeric()
}

func (t *DataTypeFLOAT) Size() int {
	return int(t.Meta.ByteSize)
}

func (t *DataTypeFLOAT) Compare(val DataType) int {
	t64, err := t.Cast(float64Meta)
	if err != nil {
		panic(errors.Wrap(err, "failed to cast 't' to float64"))
	}

	v64, err := val.Cast(float64Meta)
	if err != nil {
		panic(errors.Wrap(err, "failed to cast 'val' to float64"))
	}

	return helpers.CompareFloat(
		t64.Value().(float64),
		v64.Value().(float64),
	)
}

func (t *DataTypeFLOAT) CompareOp(operator Operator, val DataType) bool {
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

func (t *DataTypeFLOAT) Cast(meta DataTypeMeta) (DataType, error) {
	code := meta.GetCode()
	switch code {
		case TYPE_FLOAT: {
			if meta == nil {
				meta = t.Meta
			}
			return Type(meta).Set(t.Value()), nil
		}
		case TYPE_INTEGER: {
			if meta == nil {
				meta = int64Meta
			}

			val := t.Value()
			var intVal int64
			switch t.Meta.ByteSize {
				case 4: intVal = int64(val.(float32))
				case 8: intVal = int64(val.(float64))
			}
			return Type(meta).Set(intVal), nil
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
