package types

import (
	"go-dbms/pkg/types"
	"strconv"
	"strings"
)

func init() {
	parsers["FLOAT"] = func(tokens []string) types.DataTypeMeta {
		typeName := tokens[0]
		var (
			err error
			size int
		)

		parts := strings.Split(typeName, "Float")
		size, err = strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			panic(err)
		}

		return types.Meta(types.TYPE_FLOAT, size/8)
	}
}
