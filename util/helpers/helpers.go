package helpers

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
)

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
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
		cmp = bytes.Compare(a[i], b[i])
		if cmp != 0 {
			break
		}
	}
	return cmp
}

func Copy(matrix [][]byte) [][]byte {
	cp := make([][]byte, len(matrix))
	for i := range cp {
		cp[i] = make([]byte, len(matrix[i]))
		copy(cp[i], matrix[i])
	}
	return cp
}

func ParseJSONToken(word []byte) (emptyInterface interface{}, ok bool) {
	if (word[0] == '"' && word[len(word)-1] == '"') ||
		(word[0] >= '0' && word[0] <= '9') ||
		bytes.Equal(word, []byte("true")) ||
		bytes.Equal(word, []byte("false")) {
			err := json.Unmarshal(word, &emptyInterface)
			return emptyInterface, err == nil
	}
	return nil, false
}
