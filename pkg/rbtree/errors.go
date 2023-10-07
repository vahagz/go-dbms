package rbtree

import "errors"

var ErrNilPtr = errors.New("nil pointer")
var ErrNotFound = errors.New("not found")
var ErrInvalidPointer = errors.New("invalid pointer")
var ErrInvalidKeySize = errors.New("invalid key size")