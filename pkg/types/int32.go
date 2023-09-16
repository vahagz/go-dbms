package types

import "fmt"

type DataTypeINT32 struct {
	value int32
}

func (t *DataTypeINT32) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 4)
	bin.PutUint32(data, uint32(t.value))
	return data, nil
}

func (t *DataTypeINT32) UnmarshalBinary(data []byte) error {
	t.value = int32(bin.Uint32(data))
	return nil
}

func (t *DataTypeINT32) Value() interface{} {
	return t.value
}

func (t *DataTypeINT32) Set(value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("invalid set data type => %v", value)
	}

	t.value = v
	return nil
}

func (t *DataTypeINT32) GetCode() TypeCode {
	return TYPE_INT32
}

func (t *DataTypeINT32) IsFixedSize() bool {
	return true
}

func (t *DataTypeINT32) GetSize() int {
	return 4
}
