package types

import "go-dbms/pkg/types"

func init() {
	parsers["STRING"] = func(tokens []string) types.DataTypeMeta {
		return types.Meta(types.TYPE_STRING)
	}
}
