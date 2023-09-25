package pages

import (
	"bytes"
	"errors"
	"fmt"
	"go-dbms/util/helpers"
)

const (
	LeafNodeHeaderSz     = 19
	InternalNodeHeaderSz = 3

	flagLeafNode     = uint8(0x0)
	flagInternalNode = uint8(0x1)
)

// newNode initializes an in-memory leaf node and returns.
func NewNode(id uint64) *Node {
	return &Node{
		Id:    id,
		Dirty: true,
	}
}

type Entry struct {
	Key [][]byte
	Val []byte
}

// node represents an internal or leaf node in the B+ tree.
type Node struct {
	// configs for read/write
	Dirty bool

	// node data
	Id       uint64
	Next     uint64
	Prev     uint64
	Entries  []Entry
	Children []uint64
}

// search performs a binary search in the node entries for the given key
// and returns the index where it should be and a flag indicating whether
// key exists.
func (n *Node) Search(key [][]byte) (startIdx int, endIdx int, found bool) {
	startIdx = -1
	endIdx = -1

	// leftmost search
	left, right := 0, len(n.Entries)-1
	for left <= right {
		mid := (right + left) / 2

		cmp := helpers.CompareMatrix(key, n.Entries[mid].Key)
		if cmp == 0 {
			startIdx = mid
			right = mid - 1
		} else if cmp > 0 {
			left = mid + 1
		} else if cmp < 0 {
			right = mid - 1
		}
	}

	// rightmost search
	left, right = 0, len(n.Entries)-1
	for left <= right {
		mid := (right + left) / 2

		cmp := helpers.CompareMatrix(key, n.Entries[mid].Key)
		if cmp == 0 {
			endIdx = mid
			left = mid + 1
		} else if cmp > 0 {
			left = mid + 1
		} else if cmp < 0 {
			right = mid - 1
		}
	}

	// if found
	if startIdx != -1 {
		return startIdx, endIdx, true
	}

	// not found, searching index where should be inserted
	left, right = 0, len(n.Entries)-1
	for left <= right {
		mid := (right + left) / 2

		cmp := helpers.CompareMatrix(key, n.Entries[mid].Key)
		if cmp == 0 {
			return mid, mid, true
		} else if cmp > 0 {
			left = mid + 1
		} else if cmp < 0 {
			right = mid - 1
		}
	}
	return left, right, false
}

// insertChild adds the given child at appropriate location under the node.
func (n *Node) InsertChild(idx int, child *Node) {
	n.Dirty = true
	n.Children = append(n.Children, 0)
	copy(n.Children[idx+1:], n.Children[idx:])
	n.Children[idx] = child.Id
}

// insertAt inserts the entry at the given index into the node.
func (n *Node) InsertAt(idx int, e Entry) {
	n.Dirty = true
	n.Entries = append(n.Entries, Entry{})
	copy(n.Entries[idx+1:], n.Entries[idx:])
	n.Entries[idx] = e
}

// removeAt removes the entry at given index and returns the value
// that existed.
func (n *Node) RemoveAt(idx int) Entry {
	n.Dirty = true
	e := n.Entries[idx]
	n.Entries = append(n.Entries[:idx], n.Entries[idx:]...)
	return e
}

// update updates the value of the entry with given index.
func (n *Node) Update(entryIdx int, val []byte) {
	if !bytes.Equal(val, n.Entries[entryIdx].Val) {
		n.Dirty = true
		n.Entries[entryIdx].Val = val
	}
}

// isLeaf returns true if this node has no children. (i.e., it is
// a leaf node.)
func (n *Node) IsLeaf() bool { return len(n.Children) == 0 }

func (n *Node) String() string {
	s := "{"
	for _, e := range n.Entries {
		s += fmt.Sprintf("'%s' ", e.Key)
	}
	s += "} "
	s += fmt.Sprintf(
		"[id=%d, size=%d, leaf=%t, %d<-n->%d]",
		n.Id, len(n.Entries), n.IsLeaf(), n.Prev, n.Next,
	)

	return s
}

func (n *Node) Size() int {
	if n.IsLeaf() {
		sz := LeafNodeHeaderSz
		for i := 0; i < len(n.Entries); i++ {
			// 2 for the colCount size, 2 for the value size
			sz += 2 + 2 + len(n.Entries[i].Val)
			for j := 0; j < len(n.Entries[i].Key); j++ {
				// 2 for key size
				sz += 2 + len(n.Entries[i].Key[j])
			}
		}
		return sz
	}

	sz := InternalNodeHeaderSz + 4 // +4 for the extra child pointer
	for i := 0; i < len(n.Entries); i++ {
		// 4 for the child pointer, 2 for the key size
		sz += 4 + 2 + len(n.Entries[i].Key)
	}
	return sz
}

func (n *Node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.Size())
	offset := 0

	if n.IsLeaf() {
		// Note: update leafNodeHeaderSz if this is updated.
		buf[offset] = flagLeafNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.Entries)))
		offset += 2

		bin.PutUint64(buf[offset:offset+8], n.Next)
		offset += 8

		bin.PutUint64(buf[offset:offset+8], n.Prev)
		offset += 8

		for i := 0; i < len(n.Entries); i++ {
			e := n.Entries[i]

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.Val)))
			offset += 2

			copy(buf[offset:], e.Val)
			offset += len(e.Val)

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.Key)))
			offset += 2

			for j := range e.Key {
				bin.PutUint16(buf[offset:offset+2], uint16(len(e.Key[j])))
				offset += 2

				copy(buf[offset:], e.Key[j])
				offset += len(e.Key[j])
			}
		}
	} else {
		// Note: update internalNodeHeaderSz if this is updated.
		buf[offset] = flagInternalNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.Entries)))
		offset += 2

		// write the 0th pointer
		bin.PutUint64(buf[offset:offset+8], n.Children[0])
		offset += 8

		for i := 0; i < len(n.Entries); i++ {
			e := n.Entries[i]

			bin.PutUint64(buf[offset:offset+4], uint64(n.Children[i+1]))
			offset += 8

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.Key)))
			offset += 2

			for j := range e.Key {
				bin.PutUint16(buf[offset:offset+2], uint16(len(e.Key[j])))
				offset += 2
				
				copy(buf[offset:], e.Key[j])
				offset += len(e.Key[j])
			}
		}
	}
	return buf, nil
}

func (n *Node) UnmarshalBinary(d []byte) error {
	if n == nil {
		return errors.New("cannot unmarshal into nil node")
	}

	offset := 1 // (skip 0th field for flag)
	if d[0]&flagInternalNode == 0 {
		// leaf node
		entryCount := int(bin.Uint16(d[offset : offset+2]))
		offset += 2

		n.Next = bin.Uint64(d[offset : offset+8])
		offset += 8

		n.Prev = bin.Uint64(d[offset : offset+8])
		offset += 8

		for i := 0; i < entryCount; i++ {
			e := Entry{}
			
			valSz := int(bin.Uint16(d[offset : offset+2]))
			offset += 2

			e.Val = make([]byte, valSz)
			copy(e.Val, d[offset:offset+valSz])
			offset += valSz

			colCount := int(bin.Uint16(d[offset : offset+2]))
			offset += 2

			e.Key = make([][]byte, colCount)
			for j := 0; j < colCount; j++ {
				keySz := int(bin.Uint16(d[offset : offset+2]))
				offset += 2

				e.Key[j] = make([]byte, keySz)
				copy(e.Key[j], d[offset:offset+keySz])
				offset += keySz
			}

			n.Entries = append(n.Entries, e)
		}
	} else {
		// internal node
		entryCount := int(bin.Uint16(d[offset : offset+2]))
		offset += 2

		// read the left most child pointer
		n.Children = append(n.Children, bin.Uint64(d[offset:offset+8]))
		offset += 8 // we are at offset 11 now

		for i := 0; i < entryCount; i++ {
			childPtr := bin.Uint64(d[offset : offset+8])
			offset += 8

			colCount := bin.Uint16(d[offset : offset+2])
			offset += 2

			key := make([][]byte, colCount)
			for j := 0; j < int(colCount); j++ {
				keySz := bin.Uint16(d[offset : offset+2])
				offset += 2

				key[j] = make([]byte, keySz)
				copy(key[j], d[offset:])
				offset += int(keySz)
			}

			n.Children = append(n.Children, childPtr)
			n.Entries = append(n.Entries, Entry{Key: key})
		}

	}

	return nil
}
