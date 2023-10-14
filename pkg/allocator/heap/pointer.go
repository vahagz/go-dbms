package allocator

import (
	"encoding"
	"encoding/binary"
	"go-dbms/pkg/pager"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

var bin = binary.BigEndian
var ErrInvalidPointer = errors.New("invalid pointer")
var ErrUnmarshal = errors.New("unmarshal error")
var ErrMarshal = errors.New("marshal error")

const pointerSize = 12

type Pointable interface {
	Get(into encoding.BinaryUnmarshaler) error
	Set(from encoding.BinaryMarshaler) error
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type pointer struct {
	ptr   uint64
	meta  *pointerMetadata
	pager *pager.Pager
}

func (p *pointer) Get(into encoding.BinaryUnmarshaler) error {
	buf := make([]byte, p.meta.size)
	if err := p.pager.ReadAt(buf, p.ptr); err != nil {
		return ErrInvalidPointer
	}
	if err := into.UnmarshalBinary(buf); err != nil {
		return ErrUnmarshal
	}
	return nil
}

func (p *pointer) Set(from encoding.BinaryMarshaler) error {
	bytes, err := from.MarshalBinary()
	if err != nil {
		return ErrMarshal
	}
	if err := p.pager.WriteAt(bytes, p.ptr); err != nil {
		return ErrInvalidPointer
	}
	return nil
}

func (p *pointer) New() rbtree.EntryItem {
	return &pointer{meta: &pointerMetadata{}}
}

func (p *pointer) Copy() rbtree.EntryItem {
	cp := *p
	cp.meta = &pointerMetadata{}
	*cp.meta = *p.meta
	return &cp
}

func (p *pointer) Size() int {
	return pointerSize
}

func (p *pointer) IsNil() bool {
	return p == nil
}

func (p *pointer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.Size())
	bin.PutUint32(buf[0:4], p.meta.size)
	bin.PutUint64(buf[4:12], p.ptr)
	return buf, nil
}

func (p *pointer) UnmarshalBinary(d []byte) error {
	p.meta.size = bin.Uint32(d[0:4])
	p.ptr = bin.Uint64(d[4:12])
	return nil
}
