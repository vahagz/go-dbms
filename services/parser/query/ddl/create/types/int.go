package types

import (
	"go-dbms/pkg/types"
	"strconv"
	"strings"
)

func init() {
	parsers["INT"] = func(tokens []string) types.DataTypeMeta {
		typeName := tokens[0]
		var (
			err error
			signed, ai bool
			size int
		)

		parts := strings.Split(typeName, "Int")
		signed = parts[0] == "U"
		size, err = strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			panic(err)
		}

		if len(tokens) > 1 {
			ai = tokens[1] == "AUTO" && tokens[2] == "INCREMENT"
		}

		return types.Meta(types.TYPE_INTEGER, signed, size/8, ai)
	}
}
