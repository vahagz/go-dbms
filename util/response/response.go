package response

import (
	"bytes"
	"errors"
	"go-dbms/services/executor"
	"io"
)

type Reader struct {
	source io.Reader
	buf    *bytes.Buffer
	header []byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		header: make([]byte, executor.HeaderSize),
		source: r,
		buf:    &bytes.Buffer{},
	}
}

func (rr *Reader) ReadLine() (buf []byte, err error) {
	n, err := rr.read(rr.header)
	if err != nil {
		return nil, err
	} else if len(rr.header) != n {
		return nil, errors.New("header size missmatch")
	}

	messageSize := executor.Bin.Uint32(rr.header)
	message := make([]byte, messageSize)
	n, err = rr.read(message)
	if err != nil {
		return nil, err
	} else if len(message) != n {
		return nil, errors.New("message size missmatch")
	}

	return message, nil
}

func (rr *Reader) read(buf []byte) (n int, err error) {
	for rr.buf.Len() < len(buf) {
		n, err := rr.source.Read(buf)
		if err != nil {
			return 0, err
		}

		rr.buf.Write(buf[:n])
	}

	return rr.buf.Read(buf)
}
