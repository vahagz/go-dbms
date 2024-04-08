package function

import "go-dbms/pkg/types"

type intType = int64
var (
	intMeta = &types.DataTypeINTEGERMeta{Signed: true, ByteSize: 8}
)

type floatType = float64
var (
	floatMeta = &types.DataTypeFLOATMeta{ByteSize: 8}
)
