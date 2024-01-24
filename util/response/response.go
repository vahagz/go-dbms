package response

import (
	"bytes"
	"errors"
	"io"
)

type endian interface {
	Uint64(b []byte) uint64
}

type Reader struct {
	source             io.Reader
	buf                *bytes.Buffer
	header, headerCopy []byte
	byteOrder          endian
}

func NewReader(r io.Reader, hs int, byteOrder endian) *Reader {
	return &Reader{
		header:     make([]byte, hs),
		headerCopy: make([]byte, 8),
		byteOrder:  byteOrder,
		source:     r,
		buf:        &bytes.Buffer{},
	}
}

func (rr *Reader) ReadLine() (buf []byte, err error) {
	n, err := rr.read(rr.header)
	if err != nil {
		return nil, err
	} else if len(rr.header) != n {
		return nil, errors.New("header size missmatch")
	}

	copy(rr.headerCopy, rr.header)
	messageSize := rr.byteOrder.Uint64(rr.headerCopy)
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
