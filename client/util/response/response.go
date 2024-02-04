package response

import (
	"bufio"
	"encoding/binary"
	"io"
)

type Reader struct {
	source *bufio.Reader
	buf    []byte
	len    int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{source: bufio.NewReader(r)}
}

func (rr *Reader) ReadLine() (buf []byte, err error) {
	err = rr.read(4)
	if err != nil {
		return nil, err
	}

	messageSize := binary.BigEndian.Uint32(rr.buf)
	err = rr.read(int(messageSize))
	if err != nil {
		return nil, err
	}

	return rr.buf[:messageSize], nil
}

func (rr *Reader) read(n int) (err error) {
	if len(rr.buf) < n {
		rr.buf = make([]byte, n)
	}
	rr.len = 0

	for rr.len < n {
		rn, err := rr.source.Read(rr.buf[rr.len:n])
		if err != nil {
			return err
		}

		rr.len += rn
	}

	return nil
}
