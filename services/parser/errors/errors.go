package errors

import "errors"

var (
	ErrSyntax        = errors.New("syntax error")
	ErrNoSelection   = errors.New("empty 'SELECT' list")
	ErrNoFrom        = errors.New("empty 'FROM' clause")
	ErrNoWhereIndex  = errors.New("empty 'WHERE_INDEX' clause")
	ErrInvalidEngine = errors.New("invalid engine")
)
