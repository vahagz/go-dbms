package drop

import "text/scanner"

type QueryDropDatabase struct {
	*QueryDrop
	DB string `json:"db"`
}

func (qs *QueryDropDatabase) Parse(s *scanner.Scanner) (err error) {
	return nil
}
