package data

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestRecord(t *testing.T) {
	id := 1
	name := "first"

	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	data := [][]byte{
		idBytes,
		[]byte(name),
	}
	pageSize := os.Getpagesize()

	meta := metadata{
		dirty: true,
		magic: 1,
		version: 1,
		flags: 0,
		pageSz: uint32(pageSize),
		columns: []column{
			{"id", TYPE_INT},
			{"name", TYPE_STRING},
		},
		freeList: []int{2,3,4,5,6,7,8,9},
	}

	r := newRecord(1, meta)
	r.overflow = []int{}
	r.dirty = true
	r.data = data

	b, err := r.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, pageSize)
	copy(buf[:len(b)], b)
	r2 := newRecord(2, meta)
	err = r2.UnmarshalBinary(buf)
	if err != nil {
		t.Fatal(err)
	}

	decodedId := binary.LittleEndian.Uint32(r2.data[0])
	if decodedId != uint32(id) {
		t.Fatalf("id not equal (%v != %v)", decodedId, id)
	}
	
	decodedName := string(r2.data[1])
	if decodedName != name {
		t.Fatalf("id not equal (%v != %v)", decodedName, id)
	}
}