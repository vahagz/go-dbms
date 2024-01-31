package create

import (
	"go-dbms/pkg/column"
	"text/scanner"
)

type QueryCreateTable struct {
	*QueryCreate
	Database string                   `json:"database"`
	Name     string                   `json:"name"`
	Columns  []*column.Column         `json:"columns"`
	Indexes  []*QueryCreateTableIndex `json:"indexes"`
}

func (qs *QueryCreateTable) Parse(s *scanner.Scanner) (err error) {
	return nil
}
