package types

import (
	"fmt"
	"go-dbms/pkg/types"
	"regexp"
	"strings"
)

var supportedIntSizes = []string{"8","16","32","64"}
var intRegex, _ = regexp.Compile(fmt.Sprintf("^(^|U)Int(%s)$", strings.Join(supportedIntSizes, "|")))

var parsers = map[string]func(tokens []string)types.DataTypeMeta{}

func Parse(tokens []string) types.DataTypeMeta {
	typeName := tokens[0]

	ok := intRegex.Match([]byte(typeName))
	if ok {
		return parsers["INT"](tokens)
	}
	return parsers[typeName](tokens)
}
