package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"

	"go-dbms/services/parser/errors"
	"go-dbms/util/helpers"
)

func init() {
	typesMap[TYPE_VARCHAR] = newable{
		newInstance: func(meta DataTypeMeta) DataType {
			m := meta.(*DataTypeVARCHARMeta)
			return &DataTypeVARCHAR{
				value: make([]byte, m.Cap),
				Code: m.GetCode(),
				Meta: m,
			}
		},
		newMeta: func(args ...interface{}) DataTypeMeta {
			if len(args) == 0 {
				return &DataTypeVARCHARMeta{}
			}

			return &DataTypeVARCHARMeta{
				Cap: helpers.Convert[uint16](args[0]),
			}
		},
	}

	parsers["VARCHAR"] = func(tokens []string) DataTypeMeta {
		if tokens[1] != "(" || tokens[len(tokens)-1] != ")" {
			panic(errors.ErrSyntax)
		}

		cap, err := strconv.Atoi(tokens[2])
		if err != nil {
			panic(err)
		}
		return Meta(TYPE_VARCHAR, cap)
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

func (m *DataTypeVARCHARMeta) Default() DataType {
	cp := *m
	return Type(&cp).Set("")
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
	buf := make([]byte, t.Size())
	binary.BigEndian.PutUint16(buf[:2], t.Len)
	copy(buf[2:], t.value)
	return buf, nil
}

func (t *DataTypeVARCHAR) UnmarshalBinary(data []byte) error {
	t.Len = binary.BigEndian.Uint16(data[:2])
	copy(t.value, data[2:])
	return nil
}

func (t *DataTypeVARCHAR) Copy() DataType {
	return &DataTypeVARCHAR{
		value: slices.Clone(t.value),
		Code:  t.Code,
		Len:   t.Len,
		Meta:  t.MetaCopy().(*DataTypeVARCHARMeta),
	}
}

func (t *DataTypeVARCHAR) MetaCopy() DataTypeMeta {
	return &DataTypeVARCHARMeta{
		Cap: t.Meta.Cap,
	}
}

func (t *DataTypeVARCHAR) Bytes() []byte {
	return t.value[:t.Len]
}

func (t *DataTypeVARCHAR) Value() json.Token {
	return string(t.value[:t.Len])
}

func (t *DataTypeVARCHAR) Set(value interface{}) DataType {
	switch value.(type) {
	case []byte:
		t.Len = uint16(copy(t.value, value.([]byte)))
		break
	case string:
		t.Len = uint16(copy(t.value, []byte(value.(string))))
		break
	default:
		panic(fmt.Errorf("invalid set data type => %v", value))
	}

	return t
}

func (t *DataTypeVARCHAR) Fill() DataType {
	t.Len = t.Meta.Cap
	for i := range t.value {
		t.value[i] = math.MaxUint8
	}
	return t
}

func (t *DataTypeVARCHAR) Zero() DataType {
	t.Len = t.Meta.Cap
	for i := range t.value {
		t.value[i] = 0
	}
	return t
}

func (t *DataTypeVARCHAR) GetCode() TypeCode {
	return t.Code
}

func (t *DataTypeVARCHAR) Default() DataType {
	return t.Meta.Default()
}

func (t *DataTypeVARCHAR) IsFixedSize() bool {
	return t.Meta.IsFixedSize()
}

func (t *DataTypeVARCHAR) Size() int {
	return 2 + int(t.Meta.Cap) // 2 for length size
}

func (t *DataTypeVARCHAR) Compare(val DataType) int {
	return bytes.Compare(t.Bytes(), val.Bytes())
}

func (t *DataTypeVARCHAR) CompareOp(operator Operator, val DataType) bool {
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

func (t *DataTypeVARCHAR) Cast(meta DataTypeMeta) (DataType, error) {
	code := meta.GetCode()
	switch code {
		case TYPE_INTEGER: {
			if meta == nil {
				meta = &DataTypeINTEGERMeta{
					Signed: true,
					ByteSize: 4,
				}
			}
			number, _ := strconv.Atoi(string(t.value))
			return Type(meta).Set(number), nil
		}
		case TYPE_STRING, TYPE_VARCHAR: {
			if meta == nil {
				meta = t.Meta
			} else {
				if code == TYPE_VARCHAR {
					meta = &DataTypeVARCHARMeta{
						Cap: t.Meta.Cap,
					}
				} else {
					meta = &DataTypeSTRINGMeta{}
				}
			}
			return Type(meta).Set(t.Value()), nil
		}
		case TYPE_FLOAT: {
			if meta == nil {
				meta = &DataTypeFLOATMeta{
					ByteSize: 8,
				}
			}
			return Type(meta).Set(helpers.MustVal(strconv.ParseFloat(string(t.value), 64))), nil
		}
		case TYPE_DATETIME: {
			if meta == nil {
				meta = &DataTypeDATETIMEMeta{}
			}
			return Type(meta).Set(string(t.value)), nil
		}
	}

	return nil, fmt.Errorf("typecast from %v to %v not supported", t.Code, code)
}
