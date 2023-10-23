package allocator

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"go-dbms/pkg/pager"

	"github.com/pkg/errors"
)

var bin = binary.BigEndian
var ErrInvalidPointer = errors.New("invalid Pointer")
var ErrUnmarshal = errors.New("unmarshal error")
var ErrMarshal = errors.New("marshal error")

const PointerSize = 12

type binaryMarshalerUnmarshaler interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Pointable interface {
	Get(into encoding.BinaryUnmarshaler) error
	Set(from encoding.BinaryMarshaler) error
	Addr() uint64
	binaryMarshalerUnmarshaler
}

type Pointer struct {
	ptr   uint64
	meta  *pointerMetadata
	pager *pager.Pager
}

func (p *Pointer) Get(into encoding.BinaryUnmarshaler) error {
	buf := make([]byte, p.meta.size)
	if err := p.pager.ReadAt(buf, p.ptr); err != nil {
		return ErrInvalidPointer
	}
	if err := into.UnmarshalBinary(buf); err != nil {
		return ErrUnmarshal
	}
	return nil
}

func (p *Pointer) Set(from encoding.BinaryMarshaler) error {
	bytes, err := from.MarshalBinary()
	if err != nil {
		return ErrMarshal
	}
	if err := p.pager.WriteAt(bytes, p.ptr); err != nil {
		return ErrInvalidPointer
	}
	return nil
}

func (p *Pointer) Addr() uint64 {
	return p.ptr
}

func (p *Pointer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, PointerSize)
	bin.PutUint32(buf[0:4], p.meta.size)
	bin.PutUint64(buf[4:12], p.ptr)
	return buf, nil
}

func (p *Pointer) UnmarshalBinary(d []byte) error {
	p.meta.size = bin.Uint32(d[0:4])
	p.ptr = bin.Uint64(d[4:12])
	return nil
}

func (p *Pointer) Format(f fmt.State, c rune) {
	f.Write([]byte(fmt.Sprintf("{ptr:'%v', size:'%v', free:'%v'}", p.ptr, p.meta.size, p.meta.free)))
}

func (p *Pointer) key() *freelistKey {
	return &freelistKey{
		ptr:  p.ptr - pointerMetaSize,
		size: p.meta.size + 2 * pointerMetaSize,
	}
}

func (p *Pointer) next() (*Pointer, error) {
	nextPtrMeta := &pointerMetadata{}
	nextPtrMetaBytes := make([]byte, pointerMetaSize)
	err := p.pager.ReadAt(nextPtrMetaBytes, p.ptr + uint64(p.meta.size) + pointerMetaSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read next Pointer meta")
	}

	err = nextPtrMeta.UnmarshalBinary(nextPtrMetaBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal next Pointer meta")
	}

	return &Pointer{
		ptr:   p.ptr + uint64(p.meta.size) + 2 * pointerMetaSize,
		meta:  nextPtrMeta,
		pager: p.pager,
	}, nil
}

func (p *Pointer) prev() (*Pointer, error) {
	prevPtrMeta := &pointerMetadata{}
	prevPtrMetaBytes := make([]byte, pointerMetaSize)
	err := p.pager.ReadAt(prevPtrMetaBytes, p.ptr - 2 * pointerMetaSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read prev Pointer meta")
	}

	err = prevPtrMeta.UnmarshalBinary(prevPtrMetaBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal prev Pointer meta")
	}

	return &Pointer{
		ptr:   p.ptr - uint64(prevPtrMeta.size) - 2 * pointerMetaSize,
		meta:  prevPtrMeta,
		pager: p.pager,
	}, nil
}

func (p *Pointer) writeMeta() error {
	bytes, err := p.meta.MarshalBinary()
	if err != nil {
		return ErrMarshal
	}
	if err := p.pager.WriteAt(bytes, p.ptr - pointerMetaSize); err != nil {
		return ErrInvalidPointer
	}
	if err := p.pager.WriteAt(bytes, p.ptr + uint64(p.meta.size)); err != nil {
		return ErrInvalidPointer
	}
	return nil
}