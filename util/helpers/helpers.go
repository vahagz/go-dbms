package helpers

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strings"
	"time"

	"go-dbms/pkg/index"

	"golang.org/x/exp/constraints"
)

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}


func WriteALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		key, val := GenKV(i)
		_ = index.Put(key, val)
	}
	return time.Since(start), nil
}

func ReadALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		key, val := GenKV(i)

		v, err := index.Get(key)
		if err != nil {
			log.Fatalf("Get('%x') -> %v [i=%d]", key, err, i)
		}

		if v != val {
			log.Fatalf(
				"bad read for key='%x' : actual %d != expected %d",
				key, v, val,
			)
		}
	}
	return time.Since(start), nil
}

func ScanALot(scanner index.Scanner, count uint32) (time.Duration, error) {
	start := time.Now()

	c := 0
	err := scanner.Scan(nil, false, func(key []byte, actual uint64) bool {
		_, v := GenKV(uint32(c))
		c++

		if v != actual {
			log.Fatalf("value of key '%x' expected to be %d but was %d", key, v, actual)
		}
		return false
	})

	if c != int(count) {
		log.Fatalf("expected scan to process %d keys, but did only %d", count, c)
	}

	return time.Since(start), err
}

func GenKV(i uint32) ([]byte, uint64) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], i)
	return b[:], uint64(i)
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
