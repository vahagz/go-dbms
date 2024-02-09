package aggregator

import "go-dbms/pkg/types"

type AggregationCOUNT struct {
	*AggregatorBase
	Val uint64
}

func (as *AggregationCOUNT) Apply(row map[string]types.DataType) {
	as.Val++
}

func (as *AggregationCOUNT) Value() types.DataType {
	return types.Type(&types.DataTypeINTEGERMeta{ByteSize: 8}).Set(as.Val)
}
