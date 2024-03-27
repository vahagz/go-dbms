package function

import (
	"bytes"
	"errors"

	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
)

const CONCAT FunctionType = "CONCAT"

var ErrUnsupportedArgType = errors.New("unsupported arg type")

func init() {
	functions[CONCAT] = func(row types.DataRow, args []types.DataType) types.DataType {
		buf := &bytes.Buffer{}

		for _, arg := range args {
			switch arg.GetCode() {
				case types.TYPE_STRING:  buf.WriteString(arg.Value().(string))
				case types.TYPE_VARCHAR: buf.Write(arg.Value().([]byte))
				default:                 buf.WriteString(helpers.MustVal(arg.Cast(types.Meta(types.TYPE_STRING))).Value().(string))
			}
		}

		return types.Type(types.Meta(types.TYPE_STRING)).Set(buf.String())
	}
}
