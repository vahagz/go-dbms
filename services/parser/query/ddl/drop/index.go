package drop

import "text/scanner"

type QueryDropIndex struct {
	*QueryDrop
	DB    string `json:"db"`
	Table string `json:"table"`
	Index string `josn:"index"`
}

func (qs *QueryDropIndex) Parse(s *scanner.Scanner) (err error) {
	return nil
}
