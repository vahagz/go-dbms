package function

import (
	"time"

	"go-dbms/pkg/types"
)

const NOW FunctionType = "NOW"

func init() {
	functions[NOW] = func(row types.DataRow, args []types.DataType) types.DataType {
		return types.Type(types.Meta(types.TYPE_DATETIME)).Set(time.Now().Unix())
	}
}
