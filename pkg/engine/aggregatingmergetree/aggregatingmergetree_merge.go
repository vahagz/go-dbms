package aggregatingmergetree

import (
	"fmt"
	"go-dbms/pkg/index"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/aggregator"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (t *AggregatingMergeTree) MergeTreeFn(dst table.ITable, src table.ITable) {
	str := helpers.MustVal(src.FullScanByIndex(t.PrimaryKey(), false))

	pCols := t.PrimaryColumns()
	conds := make([]index.FilterCondition, len(pCols))

	for row, ok := str.Pop(); ok; row, ok = str.Pop() {
		str.Continue(true)
		fmt.Println(row["firstname"].Value(), row["lastname"].Value(), row["amount"].Value())
		for i, col := range pCols {
			conds[i] = index.FilterCondition{
				Left:  &projection.Projection{Alias: col.Name, Type: projection.IDENTIFIER},
				Right: &projection.Projection{Literal: row[col.Name], Type: projection.LITERAL},
			}
		}

		filter := &index.Filter{
			Operator:   types.Equal,
			Conditions: conds,
		}

		res, err := dst.ScanByIndex(t.PrimaryKey(), filter, nil)
		if err != nil {
			panic(errors.Wrap(err, "failed to get data from main table for merge process"))
		}

		mainRow, mainExists := res.Pop()
		res.Continue(false)

		if mainExists {
			updRow := types.DataRow{}
			for col, aggr := range t.Meta.GetAggregations() {
				ag := aggregator.New(aggr, []*projection.Projection{{
					Type:  projection.IDENTIFIER,
					Name:  col,
					Alias: col,
				}})

				ag.Apply(mainRow)
				ag.Apply(row)
				updRow[col] = ag.Value()
			}

			mainRes, err := dst.UpdateByIndex(t.PrimaryKey(), filter, nil, nil, updRow)
			if err != nil {
				panic(errors.Wrap(err, "failed to update data from main table on merge process"))
			}
			for _, ok := mainRes.Pop(); ok; _, ok = mainRes.Pop() {  }
		} else {
			in := stream.New[types.DataRow](1)
			in.Push(row)
			in.Close()
			mainRes, eg2 := dst.Insert(in)
			for _, ok := mainRes.Pop(); ok; _, ok = mainRes.Pop() {  }
			if err := eg2.Wait(); err != nil {
				panic(errors.Wrap(err, "failed to insert data into main table on merge process"))
			}
		}
	}
}
