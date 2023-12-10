// Package index defines common interface and errors for indexing implementations
// to use.
package customerrors

import (
	"errors"
)

var (
	// ErrKeyNotFound should be returned from lookup operations when the
	// lookup key is not found in index/store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyTooLarge is returned by index implementations when a key is
	// larger than a configured limit if any.
	ErrKeyTooLarge = errors.New("key is too large")

	// ErrEmptyKey should be returned by backends when an operation is
	// requested with an empty key.
	ErrEmptyKey = errors.New("empty key")

	// ErrImmutable should be returned by backends when write operation
	// (put/del) is attempted on a readonly.
	ErrImmutable = errors.New("operation not allowed in read-only mode")
	
	ErrNotFound = errors.New("not found")
)
