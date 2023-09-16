package types

import "fmt"

type DataTypeSTRING struct {
	value string
}

func (t *DataTypeSTRING) MarshalBinary() (data []byte, err error) {
	return []byte(t.value), nil
}

func (t *DataTypeSTRING) UnmarshalBinary(data []byte) error {
	t.value = string(data)
	return nil
}

func (t *DataTypeSTRING) Value() interface{} {
	return t.value
}

func (t *DataTypeSTRING) Set(value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid set data type => %v", value)
	}

	t.value = v
	return nil
}

func (t *DataTypeSTRING) GetCode() TypeCode {
	return TYPE_STRING
}

func (t *DataTypeSTRING) IsFixedSize() bool {
	return false
}

func (t *DataTypeSTRING) GetSize() int {
	return len(t.value)
}
