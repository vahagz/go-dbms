package rbtree

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBit(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Remove(path.Join(pwd, "rbtree_test.bin"))

	tree, err := Open[*freelistKey, *DummyVal](
		"rbtree_test.bin",
		&Options{
			PageSize: uint16(os.Getpagesize()),
		},
	)

	require.NoError(t, err)
	require.NotNil(t, tree)

	n := 1000
	list := make([]*freelistKey, 0, n)
	for i := 0; i < n; i++ {
		list = append(list, &freelistKey{
			ptr: uint64(i),
			size: 25 * uint32(i),
		})
		require.NoError(t, tree.Insert(&Entry[*freelistKey, *DummyVal]{
			Key: list[i],
			Val: &DummyVal{},
		}))
	}

	require.NoError(t, tree.WriteAll())
	
	for i := 0; i < n; i++ {
		fmt.Println(i)
		require.NoError(t, tree.Delete(list[i]))
	}

	require.NoError(t, tree.WriteAll())
}


type freelistKey struct {
	ptr  uint64
	size uint32
}

func (k *freelistKey) New() EntryItem {
	return &freelistKey{}
}

func (k *freelistKey) Copy() EntryItem {
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
