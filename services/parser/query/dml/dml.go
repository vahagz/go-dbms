package dml

import (
	"encoding/json"
	"errors"
	"fmt"
	"text/scanner"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
)

func Parse(s *scanner.Scanner, queryType query.QueryType) (query.Querier, error) {
	var q query.Querier

	switch queryType {
		case query.DELETE: q = &QueryDelete{}
		case query.INSERT: q = &QueryInsert{}
		case query.SELECT: q = &QuerySelect{}
		// case query.UPDATE: q = &QueryUpdate{}
		default:           return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", queryType))
	}

	return q, q.Parse(s)
}

type dataRow []types.DataType

func (dr *dataRow) UnmarshalJSON(data []byte) error {
	sl := []interface{}{}
	if err := json.Unmarshal(data, &sl); err != nil {
		return err
	}

	row := dataRow{}
	for _, item := range sl {
		row = append(row, types.ParseJSONValue(item))
	}

	*dr = row
	return nil
}

type dataMap map[string]types.DataType

func (dm *dataMap) UnmarshalJSON(data []byte) error {
	m := map[string]interface{}{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	mp := dataMap{}
	for key, item := range m {
		mp[key] = types.ParseJSONValue(item)
	}

	*dm = mp
	return nil
}

type indexFilter struct {
	Operator string  `json:"operator"`
	Value    dataMap `json:"value"`
}

type where statement.WhereStatement

type whereIndex struct {
	Name        string       `json:"name"`
	FilterStart *indexFilter `json:"filter_start"`
	FilterEnd   *indexFilter `json:"filter_end"`
}
