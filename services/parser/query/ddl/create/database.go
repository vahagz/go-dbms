package create

import "text/scanner"

type QueryCreateDatabase struct {
	*QueryCreate
	Name string `json:"name"`
}

func (qs *QueryCreateDatabase) Parse(s *scanner.Scanner) (err error) {
	return nil
}
