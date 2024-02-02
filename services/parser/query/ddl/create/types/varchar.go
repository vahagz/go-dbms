package types

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/errors"
	"strconv"
)

func init() {
	parsers["VARCHAR"] = func(tokens []string) types.DataTypeMeta {
		if tokens[1] != "(" || tokens[len(tokens)-1] != ")" {
			panic(errors.ErrSyntax)
		}

		cap, err := strconv.Atoi(tokens[2])
		if err != nil {
			panic(err)
		}
		return types.Meta(types.TYPE_VARCHAR, cap)
	}
}
