package stack

import (
	"errors"
	"sync"
)

var ErrEmptyStack = errors.New("empty stack")

type stack[T interface{}] struct {
	m sync.Mutex
	s []T
}

type Stack[T interface{}] interface {
	Push(v T)
	Pop() T
	Top() T
	Size() int
}

func New[T interface{}](initialSize int) Stack[T] {
	return &stack[T]{sync.Mutex{}, make([]T, 0, initialSize)}
}

func (s *stack[T]) Push(value T) {
	s.m.Lock()
	defer s.m.Unlock()

	s.s = append(s.s, value)
}

func (s *stack[T]) Pop() T {
	s.m.Lock()
	defer s.m.Unlock()

	l := len(s.s)
	if l == 0 {
		panic(ErrEmptyStack)
	}

	value := s.s[l-1]
	s.s = s.s[:l-1]
	return value
}

func (s *stack[T]) Top() T {
	s.m.Lock()
	defer s.m.Unlock()

	l := len(s.s)
	if l == 0 {
		panic(ErrEmptyStack)
	}

	return s.s[l-1]
}

func (s *stack[T]) Size() int {
	s.m.Lock()
	defer s.m.Unlock()

	return len(s.s)
}
