package executor

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

const HeaderSize = 4

var Bin = binary.BigEndian

var EOS = []byte(`EOS`) // end of stream

type Pipe struct {
	m  *sync.Mutex
	h  []byte
	r  *io.PipeReader
	w  *io.PipeWriter
	rw *bufio.ReadWriter
}

func newPipe(buf []byte) *Pipe {
	r, w := io.Pipe()

	p := &Pipe{
		m:  &sync.Mutex{},
		h:  make([]byte, HeaderSize),
		r:  r,
		w:  w,
		rw: bufio.NewReadWriter(
			bufio.NewReader(r),
			bufio.NewWriter(w),
		),
	}

	if len(buf) > 0 {
		go func() {
			_, err := p.Write(buf)
			if err != nil {
				panic(err)
			}
		}()
	}

	return p
}

func (p *Pipe) WriteTo(w io.Writer) (n int64, err error) {
	return io.Copy(w, p.rw.Reader)
}

func (p *Pipe) Write(data []byte) (n int, err error) {
	head := p.h
	locked := p.m.TryLock()
	if !locked {
		head = make([]byte, HeaderSize)
	}

	Bin.PutUint32(head, uint32(len(data)))

	pn, err := p.rw.Write(head)
	if err != nil {
		return pn, err
	}

	if locked {
		p.m.Unlock()
	}

	n, err = p.rw.Write(data)
	n += pn
	if err != nil {
		return n, err
	} else if bytes.Equal(data, EOS) {
		err = p.CloseWriter()
	}
	return n, err
}

func (p *Pipe) CloseWriter() error {
	p.rw.Flush()
	return p.w.Close()
}
