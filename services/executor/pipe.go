package executor

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

const HeaderSize = 4

var Bin = binary.BigEndian

var EOS = []byte(`EOS`) // end of stream

type Pipe struct {
	m    *sync.Mutex
	head []byte
	r    *io.PipeReader
	w    *io.PipeWriter
	// br   *bufio.Reader
	// bw   *bufio.Writer
}

func newPipe(buf []byte) *Pipe {
	pr, pw := io.Pipe()

	p := &Pipe{
		m:    &sync.Mutex{},
		head: make([]byte, HeaderSize),
		r:    pr,
		w:    pw,
		// br:   bufio.NewReader(pr),
		// bw:   bufio.NewWriter(pw),
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

func (p *Pipe) Read(data []byte) (n int, err error) {
	return p.r.Read(data)
}

func (p *Pipe) Write(data []byte) (n int, err error) {
	head := p.head
	locked := p.m.TryLock()
	if !locked {
		head = make([]byte, HeaderSize)
	}

	Bin.PutUint32(head, uint32(len(data)))

	pn, err := p.w.Write(head)
	if err != nil {
		return pn, err
	}

	if locked {
		p.m.Unlock()
	}

	n, err = p.w.Write(data)
	n += pn
	if err != nil {
		return n, err
	} else if bytes.Equal(data, EOS) {
		err = p.CloseWriter()
	}
	return n, err
}

func (p *Pipe) CloseReader() error {
	return p.r.Close()
}

func (p *Pipe) CloseWriter() error {
	// p.bw.Flush()
	return p.w.Close()
}
