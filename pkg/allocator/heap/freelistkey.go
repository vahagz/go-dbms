package allocator

import (
	"fmt"
	"go-dbms/pkg/rbtree"
)

type freelistKey struct {
	ptr  uint64
	size uint32
}

func (k *freelistKey) New() rbtree.EntryItem {
	return &freelistKey{}
}

func (k *freelistKey) Copy() rbtree.EntryItem {
	cp := *k
	return &cp
}

func (k *freelistKey) Size() int {
	return 12
}

func (k *freelistKey) IsNil() bool {
	return k == nil
}

func (k *freelistKey) MarshalBinary() ([]byte, error) {
	buf := make([]byte, k.Size())
	bin.PutUint32(buf[0:4], k.size)
	bin.PutUint64(buf[4:12], k.ptr)
	return buf, nil
}

func (k *freelistKey) UnmarshalBinary(d []byte) error {
	k.size = bin.Uint32(d[0:4])
	k.ptr = bin.Uint64(d[4:12])
	return nil
}

func (k *freelistKey) Format(f fmt.State, c rune) {
	f.Write([]byte(fmt.Sprintf("{ptr:'%v', size:'%v'}", k.ptr, k.size)))
}
