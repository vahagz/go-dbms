package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-dbms/util/helpers"
	"math"
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
				Cap: helpers.Convert(args[0], new(uint16)),
			}
		},
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

func (m *DataTypeVARCHARMeta) IsNumeric() bool {
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

func (t *DataTypeVARCHAR) Bytes() []byte {
	return t.value[:t.Len]
}

func (t *DataTypeVARCHAR) Value() interface{} {
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

func (t *DataTypeVARCHAR) IsNumeric() bool {
	return t.Meta.IsNumeric()
}

func (t *DataTypeVARCHAR) Size() int {
	return 2 + int(t.Meta.Cap) // 2 for length size
}

func (t *DataTypeVARCHAR) Compare(operator string, val DataType) bool {
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
