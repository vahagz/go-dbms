package helpers

import (
	"bytes"
	"os"
	"strings"

	"golang.org/x/exp/constraints"
)

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

func Min[T constraints.Ordered](numbers ...T) T {
	var min T = numbers[0]
	for _, n := range numbers {
		if n < min {
			min = n
		}
	}
	return min
}

func CreateDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func CompareMatrix(a, b [][]byte) int {
	var cmp int
	for i := range a {
		if a[i] == nil || b[i] == nil {
			break
		}

		cmp = bytes.Compare(a[i], b[i])
		if cmp != 0 {
			break
		}
	}
	return cmp
}
