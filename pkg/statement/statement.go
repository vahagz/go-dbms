package statement

import (
	"encoding/json"
	"go-dbms/pkg/types"
)

type Statement struct {
	Col string         `json:"column"`
	Op  string         `json:"operator"`
	Val types.DataType `json:"value"`
}

func (s *Statement) Column() string {
	return s.Col
}

func (s *Statement) Operator() string {
	return s.Op
}

func (s *Statement) Value() types.DataType {
	return s.Val
}

func (s *Statement) UnmarshalJSON(data []byte) error {
	st := make(map[string]interface{}, 3)
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}

	s.Col = st["column"].(string)
	s.Op = st["operator"].(string)
	s.Val = types.ParseJSONValue(st["value"])
	return nil
}
