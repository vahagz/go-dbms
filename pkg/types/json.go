package types

import "errors"

func ParseJSONValue(item interface{}) DataType {
	switch v := item.(type) {
		case float64: {
			if float64(int(v)) == v { // v is int
				return Type(Meta(TYPE_INTEGER, true, 8, false)).Set(int(v))
			} else { // v is float
				// TODO: dt = Type(Meta(TYPE_FLOAT64)).Set(v)
				return Type(Meta(TYPE_INTEGER, true, 8, false)).Set(int(v))
			}
		}
		case string: {
			return Type(Meta(TYPE_STRING)).Set(v)
		}
		default: {
			panic(errors.New("invalid item type"))
		}
	}
}
