package drop

import "text/scanner"

type QueryDropTable struct {
	*QueryDrop
	DB    string `json:"db"`
	Table string `json:"table"`
}

func (qs *QueryDropTable) Parse(s *scanner.Scanner) (err error) {
	return nil
}
