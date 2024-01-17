package executor

import (
	"encoding/binary"
	"io"
	"sync"
)

const prefixSize = 4

var bin = binary.BigEndian

var EOS = []byte(`END`) // end of stream

type Pipe struct {
	m      *sync.Mutex
	prefix []byte
	reader *io.PipeReader
	writer *io.PipeWriter
}

func newPipe(buf *[]byte) *Pipe {
	pr, pw := io.Pipe()
	p := &Pipe{
		m:      &sync.Mutex{},
		prefix: make([]byte, prefixSize),
		reader: pr,
		writer: pw,
	}

	b := *buf
	if len(b) > 0 {
		go func() {
			p.Write(b)
			if buf == &EOS {
				p.CloseWriter()
			}
		}()
	}

	return p
}

func (p *Pipe) Read(data []byte) (n int, err error) {
	return p.reader.Read(data)
}

func (p *Pipe) Write(data []byte) (n int, err error) {
	prefix := p.prefix
	locked := p.m.TryLock()
	if !locked {
		prefix = make([]byte, prefixSize)
	}

	bin.PutUint32(prefix, uint32(len(data)))

	pn, err := p.writer.Write(prefix)
	if err != nil {
		return pn, err
	}

	if locked {
		p.m.Unlock()
	}

	n, err = p.writer.Write(data)
	n += pn
	if err != nil {
		return n, err
	}
	return n, nil
}

func (p *Pipe) CloseReader() error {
	return p.reader.Close()
}

func (p *Pipe) CloseWriter() error {
	return p.writer.Close()
}
