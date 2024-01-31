package response

import (
	"encoding/binary"
	"io"
)

type Reader struct {
	src io.Reader
	buf []byte
	len int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{src: r}
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
		rn, err := rr.src.Read(rr.buf[rr.len:n-rr.len])
		if err != nil {
			return err
		}

		rr.len += rn
	}

	return nil
}
