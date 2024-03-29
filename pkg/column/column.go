package column

import (
	"encoding/json"

	"go-dbms/pkg/types"
)

type Column struct {
	Name string             `json:"name"`
	Typ  types.TypeCode     `json:"type"`
	Meta types.DataTypeMeta `json:"meta"`
}

type column struct {
	Name string          `json:"name"`
	Typ  types.TypeCode  `json:"type"`
	Meta json.RawMessage `json:"meta"`
}

func New(name string, meta types.DataTypeMeta) *Column {
	return &Column{
		Name: name,
		Typ:  meta.GetCode(),
		Meta: meta,
	}
}

func (c *Column) UnmarshalJSON(data []byte) error {
	col := &column{}
	if err := json.Unmarshal(data, col); err != nil {
		return err
	}

	c.Name = col.Name
	c.Typ = col.Typ
	c.Meta = types.Meta(col.Typ)
	return json.Unmarshal(col.Meta, c.Meta)
}
