package function

import "go-dbms/pkg/types"

type intType = int64
var (
	intMeta = &types.DataTypeINTEGERMeta{Signed: true, ByteSize: 8}
)

// type floatType = float64
// var (
// 	floatCode = types.TYPE_INTEGER
// 	floatMeta = &types.DataTypeINTEGERMeta{Signed: true, ByteSize: 8}
// )
