package types

import (
	"fmt"
	"regexp"
	"strings"

	"go-dbms/pkg/types"
)

var supportedIntSizes = []string{"8","16","32","64"}
var supportedFloatSizes = []string{"32","64"}
var intRegex, _ = regexp.Compile(fmt.Sprintf("^(^|U)Int(%s)$", strings.Join(supportedIntSizes, "|")))
var floatRegex, _ = regexp.Compile(fmt.Sprintf("^Float(%s)$", strings.Join(supportedFloatSizes, "|")))

var parsers = map[string]func(tokens []string)types.DataTypeMeta{}

func Parse(tokens []string) types.DataTypeMeta {
	typeName := tokens[0]

	if ok := intRegex.Match([]byte(typeName)); ok {
		return parsers["INT"](tokens)
	} else if ok := floatRegex.Match([]byte(typeName)); ok {
		return parsers["FLOAT"](tokens)
	}
	return parsers[typeName](tokens)
}
