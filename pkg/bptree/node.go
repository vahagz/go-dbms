package bptree

import (
	"bytes"
	"fmt"
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

const (
	leafNodeHeaderSz     = 3 + 3 * allocator.PointerSize
	internalNodeHeaderSz = 3 + 2 * allocator.PointerSize

	flagLeafNode     = uint8(0b00000000)
	flagInternalNode = uint8(0b00000001)
)

func internalNodeSize(degree, keySize, keyCols int) int {
	return internalNodeHeaderSz + (degree - 1) * (2 + allocator.PointerSize + keySize + keyCols * 2)
}

func leafNodeSize(degree, keySize, keyCols, valSize int) int {
	return leafNodeHeaderSz + (degree - 1) * (4 + valSize + keySize + keyCols * 2)
}

type entry struct {
	key [][]byte
	val []byte
}

// node represents an internal or leaf node in the B+ tree.
type node struct {
	// configs for read/write
	dirty bool
	meta  *metadata

	// node data
	dummyPtr allocator.Pointable
	right    allocator.Pointable
	left     allocator.Pointable
	parent   allocator.Pointable
	entries  []entry
	children []allocator.Pointable
}

func (n *node) IsDirty() bool {
	return n.dirty
}

func (n *node) Dirty(v bool) {
	n.dirty = v
}

func (n *node) IsNil() bool {
	return n == nil
}

func (n *node) IsFull() bool {
	return len(n.entries) >= int(n.meta.degree)
}

// search performs a binary search in the node entries for the given key
// and returns the index where it should be and a flag indicating whether
// key exists.
func (n *node) search(key [][]byte) (idx int, found bool) {
	left, right := 0, len(n.entries)-1

	for left <= right {
		idx = (right + left) / 2

		cmp := helpers.CompareMatrix(key, n.entries[idx].key)
		if cmp == 0 {
			if n.isLeaf() {
				return idx, true
			}
			return idx + 1, true
		} else if cmp > 0 {
			left = idx + 1
		} else if cmp < 0 {
			right = idx - 1
		}
	}

	return left, false
}

// insertChild adds the given child at appropriate location under the node.
func (n *node) insertChild(idx int, childPtr allocator.Pointable) {
	n.Dirty(true)
	n.children = append(n.children, nil)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = childPtr
}

// insertAt inserts the entry at the given index into the node.
func (n *node) insertEntry(idx int, e entry) {
	n.Dirty(true)
	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = e
}

func (n *node) appendEntry(e entry) {
	n.Dirty(true)
	n.entries = append(n.entries, e)
}

func (n *node) appendChild(p allocator.Pointable) {
	n.Dirty(true)
	n.children = append(n.children, p)
}

// removeAt removes the entry at given index and returns the value
// that existed.
func (n *node) removeEntries(from, to int) []entry {
	n.Dirty(true)
	e := append(make([]entry, 0, to-from), n.entries[from:to]...)
	n.entries = append(n.entries[:from], n.entries[to:]...)
	return e
}

func (n *node) removeChildren(from, to int) []allocator.Pointable {
	n.Dirty(true)
	p := append(make([]allocator.Pointable, 0, to-from), n.children[from:to]...)
	n.children = append(n.children[:from], n.children[to:]...)
	return p
}

// update updates the value of the entry with given index.
func (n *node) update(entryIdx int, val []byte) {
	if !bytes.Equal(val, n.entries[entryIdx].val) {
		n.Dirty(true)
		n.entries[entryIdx].val = val
	}
}

// isLeaf returns true if this node has no children. (i.e., it is
// a leaf node.)
func (n *node) isLeaf() bool { return len(n.children) == 0 }

func (n *node) String() string {
	s := "{"
	for _, e := range n.entries {
		s += fmt.Sprintf("'%s' ", e.key)
	}
	s += "} "
	s += fmt.Sprintf(
		"[size=%d, leaf=%t, %d<-n->%d]",
		len(n.entries), n.isLeaf(), n.left, n.right,
	)

	return s
}

func (n *node) size() int {
	sz := 0
	if n.isLeaf() {
		sz += leafNodeHeaderSz
	} else {
		sz += internalNodeHeaderSz
	}

	for i := 0; i < len(n.entries); i++ {
		if n.isLeaf() {
			// 2 for the colCount size, 2 for the value size
			sz += 2 + 2 + len(n.entries[i].val)
		} else {
			// 8 for the child pointer, 2 for the colCount
			sz += allocator.PointerSize + 2
		}

		for j := 0; j < len(n.entries[i].key); j++ {
			// 2 for key size
			sz += 2 + len(n.entries[i].key[j])
		}
	}

	return sz
}

func (n *node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.size())
	offset := 0

	if n.isLeaf() {
		nextBytes, err := n.right.MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal next ptr")
		}
		
		prevBytes, err := n.left.MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal prev ptr")
		}

		parentBytes, err := n.parent.MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal parent ptr")
		}

		// Note: update leafNodeHeaderSz if this is updated.
		buf[offset] = flagLeafNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.entries)))
		offset += 2

		copy(buf[offset:], nextBytes)
		offset += allocator.PointerSize

		copy(buf[offset:], prevBytes)
		offset += allocator.PointerSize
		
		copy(buf[offset:], parentBytes)
		offset += allocator.PointerSize

		for i := 0; i < len(n.entries); i++ {
			e := n.entries[i]

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.val)))
			offset += 2

			copy(buf[offset:], e.val)
			offset += len(e.val)

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.key)))
			offset += 2

			for j := range e.key {
				bin.PutUint16(buf[offset:offset+2], uint16(len(e.key[j])))
				offset += 2

				copy(buf[offset:], e.key[j])
				offset += len(e.key[j])
			}
		}
	} else {
		// Note: update internalNodeHeaderSz if this is updated.
		buf[offset] = flagInternalNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.entries)))
		offset += 2

		// write the 0th pointer
		extraChildPtrBytes, err := n.children[0].MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal extra child ptr")
		}
		
		parentBytes, err := n.parent.MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal extra child ptr")
		}

		copy(buf[offset:], extraChildPtrBytes)
		offset += allocator.PointerSize

		copy(buf[offset:], parentBytes)
		offset += allocator.PointerSize

		for i := 0; i < len(n.entries); i++ {
			e := n.entries[i]

			childBytes, err := n.children[i+1].MarshalBinary()
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal child ptr")
			}

			copy(buf[offset:], childBytes)
			offset += allocator.PointerSize

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.key)))
			offset += 2

			for j := range e.key {
				bin.PutUint16(buf[offset:offset+2], uint16(len(e.key[j])))
				offset += 2
				
				copy(buf[offset:], e.key[j])
				offset += len(e.key[j])
			}
		}
	}
	return buf, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	if n == nil {
		return errors.New("cannot unmarshal into nil node")
	}

	offset := 1 // (skip 0th field for flag)
	if d[0]&flagInternalNode == 0 {
		// leaf node
		entryCount := int(bin.Uint16(d[offset:offset+2]))
		offset += 2

		err := n.right.UnmarshalBinary(d[offset:offset+allocator.PointerSize])
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal pointer")
		}
		offset += allocator.PointerSize

		err = n.left.UnmarshalBinary(d[offset:offset+allocator.PointerSize])
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal pointer")
		}
		offset += allocator.PointerSize

		err = n.parent.UnmarshalBinary(d[offset:offset+allocator.PointerSize])
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal pointer")
		}
		offset += allocator.PointerSize

		for i := 0; i < entryCount; i++ {
			e := entry{}
			
			valSz := int(bin.Uint16(d[offset:offset+2]))
			offset += 2

			e.val = make([]byte, valSz)
			copy(e.val, d[offset:offset+valSz])
			offset += valSz

			colCount := int(bin.Uint16(d[offset:offset+2]))
			offset += 2

			e.key = make([][]byte, colCount)
			for j := 0; j < colCount; j++ {
				keySz := int(bin.Uint16(d[offset:offset+2]))
				offset += 2

				e.key[j] = make([]byte, keySz)
				copy(e.key[j], d[offset:offset+keySz])
				offset += keySz
			}

			n.entries = append(n.entries, e)
		}
	} else {
		// internal node
		entryCount := int(bin.Uint16(d[offset:offset+2]))
		offset += 2

		// read the left most child pointer
		n.children = append(n.children, n.dummyPtr.Copy())
		err := n.children[len(n.children)-1].UnmarshalBinary(d[offset:offset+allocator.PointerSize])
		offset += allocator.PointerSize
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal left most child ptr")
		}

		n.parent = n.dummyPtr.Copy()
		err = n.parent.UnmarshalBinary(d[offset:offset+allocator.PointerSize])
		offset += allocator.PointerSize
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal parent ptr")
		}

		for i := 0; i < entryCount; i++ {
			childPtr := n.dummyPtr.Copy()
			err := childPtr.UnmarshalBinary(d[offset:offset+allocator.PointerSize])
			offset += allocator.PointerSize
			if err != nil {
				return errors.Wrap(err, "failed to unmarshal child ptr")
			}

			colCount := bin.Uint16(d[offset:offset+2])
			offset += 2

			key := make([][]byte, colCount)
			for j := 0; j < int(colCount); j++ {
				keySz := bin.Uint16(d[offset:offset+2])
				offset += 2

				key[j] = make([]byte, keySz)
				copy(key[j], d[offset:])
				offset += int(keySz)
			}

			n.children = append(n.children, childPtr)
			n.entries = append(n.entries, entry{key: key})
		}
	}

	return nil
}
