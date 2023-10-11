package freelist

import (
	"fmt"
	"go-dbms/pkg/rbtree"
	"go-dbms/util/helpers"

	"golang.org/x/exp/constraints"
)

const itemSize = 10

func newEntry[K, V constraints.Unsigned](key K, val V) *rbtree.Entry[*Item[K], *Item[V]] {
	return &rbtree.Entry[*Item[K], *Item[V]]{
		Key: &Item[K]{key},
		Val: &Item[V]{val},
	}
}

func newKey[T constraints.Unsigned](val T) *Item[T] {
	return &Item[T]{val}
}

type Item[T constraints.Unsigned] struct {
	Val T
}

func (p *Item[T]) New() rbtree.EntryItem { return &Item[T]{} }
func (p *Item[T]) Size() int             { return helpers.Sizeof(p.Val) }
func (p *Item[T]) IsNil() bool           { return p == nil }

func (p *Item[T]) MarshalBinary() ([]byte, error) {
	bytes := helpers.Bytesof(p.Val)
	fmt.Println(bytes)
	buf := make([]byte, len(bytes))
	copy(buf, bytes)
	return buf, nil
}

func (p *Item[T]) UnmarshalBinary(d []byte) error {
	helpers.Frombytes(d, &p.Val)
	return nil
}
