package types

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"go-dbms/util/helpers"
)

var datetimeMeta = &DataTypeDATETIMEMeta{}

func init() {
	typesMap[TYPE_DATETIME] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			m := meta.(*DataTypeDATETIMEMeta)
			return &DataTypeDATETIME{
				value: 0,
				DataTypeBASE: DataTypeBASE[*DataTypeDATETIMEMeta]{
					Code: m.GetCode(),
					Meta: m,
				},
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeDATETIMEMeta{}
			}

			return &DataTypeDATETIMEMeta{}
		},
	}

	parsers["DATETIME"] = func(tokens []string) DataTypeMeta {
		return Meta(TYPE_DATETIME)
	}
}

type DataTypeDATETIMEMeta struct {
}

func (m *DataTypeDATETIMEMeta) GetCode() TypeCode {
	return TYPE_DATETIME
}

func (m *DataTypeDATETIMEMeta) Size() int {
	return 8
}

func (m *DataTypeDATETIMEMeta) Default() DataType {
	return Type(&DataTypeDATETIMEMeta{}).Set(0)
}

func (m *DataTypeDATETIMEMeta) IsFixedSize() bool {
	return true
}


type DataTypeDATETIME struct {
	value uint64
	DataTypeBASE[*DataTypeDATETIMEMeta]
}

func (t *DataTypeDATETIME) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 8)
	binary.BigEndian.PutUint64(data, t.value)
	return data, nil
}

func (t *DataTypeDATETIME) UnmarshalBinary(data []byte) error {
	t.value = binary.BigEndian.Uint64(data)
	return nil
}

func (t *DataTypeDATETIME) Copy() DataType {
	return &DataTypeDATETIME{
		value: t.value,
		DataTypeBASE: DataTypeBASE[*DataTypeDATETIMEMeta]{
			Code: t.GetCode(),
			Meta: t.MetaCopy().(*DataTypeDATETIMEMeta),
		},
	}
}

func (t *DataTypeDATETIME) MetaCopy() DataTypeMeta {
	return &DataTypeDATETIMEMeta{}
}

func (t *DataTypeDATETIME) Bytes() []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, t.value)
	return data
}

func (t *DataTypeDATETIME) Value() json.Token {
	return helpers.FormatTime(time.Unix(int64(t.value), 0))
}

func (t *DataTypeDATETIME) Set(value interface{}) DataType {
	if vs, ok := value.(string); ok {
		value = helpers.MustVal(helpers.ParseTime(vs)).Unix()
	}

	switch v := value.(type) {
		case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
			t.value = helpers.Convert[uint64](v)
		default:
			fmt.Printf("%T,%v", v,v)
			panic(ErrInvalidDataType)
	}
	return t
}

func (t *DataTypeDATETIME) Fill() DataType {
	t.value = math.MaxUint64
	return t
}

func (t *DataTypeDATETIME) Zero() DataType {
	t.value = 0
	return t
}

func (t *DataTypeDATETIME) Size() int {
	return t.Meta.Size()
}

func (t *DataTypeDATETIME) Compare(val DataType) int {
	v2 := val.(*DataTypeDATETIME).value
	if t.value > v2 {
		return 1
	} else if t.value < v2 {
		return -1
	}
	return 0
}

func (t *DataTypeDATETIME) CompareOp(operator Operator, val DataType) bool {
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

func (t *DataTypeDATETIME) Cast(meta DataTypeMeta) (DataType, error) {
	code := meta.GetCode()
	switch code {
		case TYPE_INTEGER: {
			return Type(meta).Set(t.value), nil
		}
		case TYPE_STRING, TYPE_VARCHAR: {
			if code == TYPE_VARCHAR {
				meta = &DataTypeVARCHARMeta{
					Cap: uint16(19),
				}
			}
			return Type(meta).Set(helpers.FormatTime(time.Unix(int64(t.value), 0))), nil
		}
	}

	return nil, fmt.Errorf("typecast from %v to %v not supported", t.Code, code)
}
