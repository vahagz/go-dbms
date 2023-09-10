package data

import "errors"

const (
	// 4 bit integer
	TYPE_INT = 0x01
	// variable length string
	TYPE_STRING = 0x02
)

func isFixedSize(typeCode uint8) bool {
	switch typeCode {
	case TYPE_STRING:
		return false
	default:
		return true
	}
}

func getSize(typeCode uint8) (size int, variableLength bool) {
	if !isFixedSize(typeCode) {
		size = -1
		variableLength = true
		return
	}

	switch typeCode {
	case TYPE_INT:
		return 4, false
	}

	panic(errors.New("invalid data type"))
}
