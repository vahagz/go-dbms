package function

import (
	"bytes"
	"errors"
	"text/scanner"

	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
)

const CAST FunctionType = "CAST"

var ErrInvalidCastType = errors.New("invalid cast type")

func init() {
	functions[CAST] = func(row types.DataRow, args []types.DataType) types.DataType {
		if args[1].GetCode() != types.TYPE_STRING {
			panic(ErrInvalidCastType)
		}

		s := &scanner.Scanner{}
		s.Init(bytes.NewBufferString(args[1].Value().(string)))
		tokens := []string{}
		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			tokens = append(tokens, s.TokenText())
		}

		return helpers.MustVal(args[0].Cast(types.Parse(tokens)))
	}
}
