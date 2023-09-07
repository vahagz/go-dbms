package stl

import (
	"errors"
)

type stack[T interface{}] struct {
	s []T
}

type Stack[T interface{}] interface {
	Push(v T)
	Pop() (T, error)
	Top() (T, error)
}

func NewStack[T interface{}]() Stack[T] {
	return &stack[T]{make([]T, 0)}
}

func (s *stack[T]) Push(value T) {
	s.s = append(s.s, value)
}

func (s *stack[T]) Pop() (value T, err error) {
	l := len(s.s)
	if l == 0 {
		err = errors.New("Empty Stack")
		return
	}

	value = s.s[l-1]
	s.s = s.s[:l-1]
	return value, nil
}

func (s *stack[T]) Top() (value T, err error) {
	l := len(s.s)
	if l == 0 {
		err = errors.New("Empty Stack")
		return
	}

	value = s.s[l-1]
	return value, nil
}