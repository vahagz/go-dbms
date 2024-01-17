package create

import "go-dbms/pkg/index"

type QueryCreateTableIndex struct {
	*index.IndexOptions
	Name string `json:"name"`
}

type QueryCreateIndex struct {
	*QueryCreate
	QueryCreateTableIndex
}
