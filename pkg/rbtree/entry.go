package rbtree

import (
	"encoding"

	"github.com/pkg/errors"
)

type Entry[K, V EntryItem] struct {
	Key K
	Val V
}

type EntryItem interface {
	New() EntryItem
	Copy() EntryItem
	Size() int
	IsNil() bool
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

func (e *Entry[K, V]) new() *Entry[K, V] {
	return &Entry[K, V]{
		Key: e.Key.New().(K),
		Val: e.Val.New().(V),
	}
}

func (e *Entry[K, V]) Size() int {
	return e.Key.Size() + e.Val.Size()
}

func (e *Entry[K, V]) Copy() *Entry[K, V] {
	cp := e.new()
	cp.Key = e.Key.Copy().(K)
	cp.Val = e.Val.Copy().(V)
	return cp
}

func (e *Entry[K, V]) MarshalBinary() ([]byte, error) {
	buf := make([]byte, e.Size())

	k, err := e.Key.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal entry Key")
	}

	v, err := e.Val.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal entry Val")
	}

	copy(buf[:len(k)], k)
	copy(buf[len(k):], v)
	return buf, nil
}

func (e *Entry[K, V]) UnmarshalBinary(d []byte) error {
	if err := e.Key.UnmarshalBinary(d[:e.Key.Size()]); err != nil {
		return errors.Wrap(err, "failed to unmarshal entry Key")
	}

	if err := e.Val.UnmarshalBinary(d[e.Key.Size():]); err != nil {
		return errors.Wrap(err, "failed to unmarsha; entry Val")
	}

	return nil
}






type DummyVal struct{}
func (v *DummyVal) New() EntryItem {return &DummyVal{}}
func (v *DummyVal) Copy() EntryItem {return &DummyVal{}}
func (v *DummyVal) Size() int {return 0}
func (v *DummyVal) IsNil() bool {return v == nil}
func (v *DummyVal) MarshalBinary() ([]byte, error) {return nil, nil}
func (v *DummyVal) UnmarshalBinary([]byte) error {return nil}
