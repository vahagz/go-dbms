package rbtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-dbms/pkg/pager"
	"go-dbms/pkg/stack"
	"math"

	"github.com/pkg/errors"
)

var bin = binary.LittleEndian

func Open(fileName string, opts *Options) (*RBTree, error) {
	p, err := pager.Open(fileName, int(opts.PageSize), false, 0664)
	if err != nil {
		return nil, errors.Wrap(err, "failed to Open rbtree")
	}

	tree := &RBTree{
		file:     fileName,
		pager:    p,
		pages:    map[uint32]*page{},
		degree:   opts.PageSize / (nodeFixedSize + opts.KeySize),
		nodeSize: nodeFixedSize + opts.KeySize,
		meta:     &metadata{},
	}

	if err := tree.open(opts); err != nil {
		_ = tree.Close()
		return nil, errors.Wrap(err, "failed to open tree")
	}

	return tree, nil
}

type RBTree struct {
	file     string
	pager    *pager.Pager
	pages    map[uint32]*page // node cache to avoid IO
	meta     *metadata        // metadata about tree structure
	degree   uint16           // number of nodes per page
	nodeSize uint16
}

func (tree *RBTree) Insert(k []byte) error {
	if err := tree.InsertMem(k); err != nil {
		return err
	}
	return errors.Wrap(tree.writeAll(), "failed to write all")
}

func (tree *RBTree) InsertMem(k []byte) error {
	if len(k) != int(tree.meta.nodeKeySize) {
		return errors.Wrap(ErrInvalidKeySize, "insert key size missmatch")
	}

	n, err := tree.alloc()
	if err != nil {
		return errors.Wrap(err, "failed to alloc 1 node")
	}

	tree.fetch(n).left = tree.meta.nullPtr
	tree.fetch(n).right = tree.meta.nullPtr
	tree.fetch(n).setRed()
	copy(tree.fetch(n).key, k)
	tree.insert(n)
	return nil
}

func (tree *RBTree) Get(k []byte) ([]byte, error) {
	if len(k) != int(tree.meta.nodeKeySize) {
		return nil, errors.Wrap(ErrInvalidKeySize, "insert key size missmatch")
	}

	ptr, err := tree.get(k)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find key")
	}
	return tree.fetch(ptr).key, nil
}

func (tree *RBTree) Delete(k []byte) error {
	if err := tree.DeleteMem(k); err != nil {
		return err
	}
	return errors.Wrap(tree.writeAll(), "failed to write all")
}

func (tree *RBTree) DeleteMem(k []byte) error {
	if len(k) != int(tree.meta.nodeKeySize) {
		return errors.Wrap(ErrInvalidKeySize, "delete key size missmatch")
	}

	ptr, err := tree.get(k)
	if err != nil {
		return errors.Wrapf(err, "failed to find key to delete => %v", k)
	}

	copy(tree.fetch(ptr).key, k)
	tree.delete(ptr)
	return nil
}

func (tree *RBTree) Scan(k []byte, scanFn func(key []byte) (bool, error)) error {
	if tree.meta.rootPtr == tree.meta.nullPtr {
		return nil
	}

	curr := tree.meta.rootPtr
	if k != nil {
		var err error
		curr, err = tree.get(k)
		if err != nil && err != ErrNotFound {
			return errors.Wrap(err, "failed to find key")
		}
	}

	s := stack.New[uint32](tree.height())
	for curr != 0 && curr != tree.meta.nullPtr || s.Size() > 0 {
		for curr != 0 && curr != tree.meta.nullPtr {
			s.Push(curr)
			if tree.fetch(curr).left == tree.meta.nullPtr {
				break
			}

			curr = tree.fetch(curr).left
		}

		curr = s.Pop();
		stop, err := scanFn(tree.fetch(curr).key)
		if stop || err != nil {
			return err
		}

		if tree.fetch(curr).right == tree.meta.nullPtr {
			curr = 0
		} else {
			curr = tree.fetch(curr).right
		}
	}

	return nil
}

func (tree *RBTree) Count() int {
	return int(tree.meta.count)
}

func (tree *RBTree) Print(count int) error {
	return tree.print(tree.meta.rootPtr, 0, count)
}

func (tree *RBTree) print(root uint32, space int, count int) error {
	// count := tree.pager.Count()
	// for i := uint32(1); i < uint32(count); i++ {
	// 	p := tree.page(i)
	// 	if err := tree.pager.Unmarshal(uint64(p.id), p); err != nil {
	// 		return errors.Wrap(err, "failed to unmarshal page")
	// 	}
	// 	nodes := p.nodes[:tree.meta.count]
	// 	fmt.Println(nodes)
	// }
	// return nil

	if root == 0 {
		return nil
	}

	// Increase distance between levels
	space += count

	// Process right child first
	if root != tree.meta.nullPtr {
		tree.print(tree.fetch(root).right, space, count)
	}

	// Print current node after space
	// count
	fmt.Println()
	for i := count; i < space; i++ {
		fmt.Print(" ")
	}
	fmt.Println(binary.BigEndian.Uint16(tree.fetch(root).key), tree.fetch(root).getFlag(FT_COLOR))

	// Process left child
	if root != tree.meta.nullPtr {
		tree.print(tree.fetch(root).left, space, count)
	}
	return nil
}

func (tree *RBTree) WriteAll() error {
	return tree.writeAll()
}

func (tree *RBTree) Close() error {
	if tree.pager == nil {
		return nil
	}

	_ = tree.writeAll()
	err := tree.pager.Close()
	tree.pager = nil
	return errors.Wrap(err, "failed to close RBTree")
}

func (tree *RBTree) get(k []byte) (uint32, error) {
	ptr := tree.meta.rootPtr
	for ptr != tree.meta.nullPtr {
		cmp := bytes.Compare(tree.fetch(ptr).key, k)
		if cmp == -1 {
			ptr = tree.fetch(ptr).right
		} else if cmp == 1 {
			ptr = tree.fetch(ptr).left
		} else {
			return ptr, nil
		}
	}
	return ptr, ErrNotFound
}

func (tree *RBTree) height() int {
	return 2 * int(math.Ceil(math.Log2(float64(tree.meta.count)))) + 1
}

func (tree *RBTree) fixDelete(x uint32) {
	for x != tree.meta.rootPtr && tree.fetch(x).isBlack() {
		if x == tree.fetch(tree.fetch(x).parent).left {
			w := tree.fetch(tree.fetch(x).parent).right

			if tree.fetch(w).isRed() { // case 1
				tree.fetch(w).setBlack()
				tree.fetch(tree.fetch(x).parent).setRed()

				tree.leftRotate(tree.fetch(x).parent)
				w = tree.fetch(tree.fetch(x).parent).right
			}

			if tree.fetch(tree.fetch(w).left).isBlack() && tree.fetch(tree.fetch(w).right).isBlack() { // case 2
				tree.fetch(w).setRed()
				x = tree.fetch(x).parent
			} else { // case 3, 4
				if tree.fetch(tree.fetch(w).right).isBlack() { // case 3
					tree.fetch(tree.fetch(w).left).setBlack()
					tree.fetch(w).setRed()

					tree.rightRotate(w)
					w = tree.fetch(tree.fetch(x).parent).right
				}

				// case 4
				tree.fetch(w).setFlag(FT_COLOR, tree.fetch(tree.fetch(x).parent).getFlag(FT_COLOR))
				tree.fetch(tree.fetch(x).parent).setBlack()
				tree.fetch(tree.fetch(w).right).setBlack()

				tree.leftRotate(tree.fetch(x).parent)
				x = tree.meta.rootPtr
			}
		} else {
			w := tree.fetch(tree.fetch(x).parent).left

			if tree.fetch(w).isRed() { // case 1
				tree.fetch(w).setBlack()
				tree.fetch(tree.fetch(x).parent).setRed()

				tree.rightRotate(tree.fetch(x).parent)
				w = tree.fetch(tree.fetch(x).parent).left
			}

			if tree.fetch(tree.fetch(w).right).isBlack() && tree.fetch(tree.fetch(w).left).isBlack() { // case 2
				tree.fetch(w).setRed()
				x = tree.fetch(x).parent
			} else { // case 3, 4
				if tree.fetch(tree.fetch(w).left).isBlack() { // case 3
					tree.fetch(tree.fetch(w).right).setBlack()
					tree.fetch(w).setRed()

					tree.leftRotate(w)
					w = tree.fetch(tree.fetch(x).parent).left
				}

				// case 4
				tree.fetch(w).setFlag(FT_COLOR, tree.fetch(tree.fetch(x).parent).getFlag(FT_COLOR))
				tree.fetch(tree.fetch(x).parent).setBlack()
				tree.fetch(tree.fetch(w).left).setBlack()

				tree.rightRotate(tree.fetch(x).parent)
				x = tree.meta.rootPtr
			}
		}
	}

	tree.fetch(x).setBlack()
}

func (tree *RBTree) delete(z uint32) {
	var x uint32
	y := z
	yOriginalColor := tree.fetch(y).getFlag(FT_COLOR)

	if tree.fetch(z).left == tree.meta.nullPtr { // no children or only right
		x = tree.fetch(z).right
		tree.transplant(z, x)
	} else if tree.fetch(z).right == tree.meta.nullPtr { // only left child
		x = tree.fetch(z).left
		tree.transplant(z, x)
	} else { // both children
		y = tree.minimum(tree.fetch(z).right)
		yOriginalColor = tree.fetch(y).getFlag(FT_COLOR)
		x = tree.fetch(y).right

		if tree.fetch(y).parent == z { // y is direct child of z
			tree.fetch(x).dirty = true
			tree.fetch(x).parent = y
		} else {
			tree.transplant(y, x)
			tree.fetch(y).dirty = true
			tree.fetch(y).right = tree.fetch(z).right
			tree.fetch(tree.fetch(y).right).dirty = true
			tree.fetch(tree.fetch(y).right).parent = y
		}

		tree.transplant(z, y)

		tree.fetch(y).dirty = true
		tree.fetch(y).left = tree.fetch(z).left
		tree.fetch(tree.fetch(y).left).dirty = true
    tree.fetch(tree.fetch(y).left).parent = y
    tree.fetch(y).setFlag(FT_COLOR, tree.fetch(z).getFlag(FT_COLOR))
	}

	if yOriginalColor == FV_COLOR_BLACK {
		tree.fixDelete(x)
	}

	tree.free(z)
	tree.meta.dirty = true
	tree.meta.count--
}

func (tree *RBTree) minimum(x uint32) uint32 {
	for tree.fetch(x).left != tree.meta.nullPtr {
		x = tree.fetch(x).left
	}
	return x
}

func (tree *RBTree) transplant(u, v uint32) {
	if tree.fetch(u).parent == tree.meta.nullPtr { // u is root
		tree.meta.dirty = true
		tree.meta.rootPtr = v
	} else {
		tree.fetch(tree.fetch(u).parent).dirty = true
		if u == tree.fetch(tree.fetch(u).parent).left { // u is left child
			tree.fetch(tree.fetch(u).parent).left = v
		} else { // u is right child
			tree.fetch(tree.fetch(u).parent).right = v
		}
	}

	tree.fetch(v).dirty = true
	tree.fetch(v).parent = tree.fetch(u).parent
}

func (tree *RBTree) fixInsert(z uint32) {
	for tree.fetch(tree.fetch(z).parent).isRed() {
		if tree.fetch(z).parent == tree.fetch(tree.fetch(tree.fetch(z).parent).parent).left { // first 3 cases
			y := tree.fetch(tree.fetch(tree.fetch(z).parent).parent).right // z uncle

			// first subcase
			if tree.fetch(y).isRed() {
				tree.fetch(tree.fetch(z).parent).setBlack()
				tree.fetch(y).setBlack()
				tree.fetch(tree.fetch(tree.fetch(z).parent).parent).setRed()
				z = tree.fetch(tree.fetch(z).parent).parent
			} else { // second and third subcases
				if z == tree.fetch(tree.fetch(z).parent).right { // second subcase, turning to third
					z = tree.fetch(z).parent
					tree.leftRotate(z)
				}
	
				// third case
				tree.fetch(tree.fetch(z).parent).setBlack()
				tree.fetch(tree.fetch(tree.fetch(z).parent).parent).setRed()
				tree.rightRotate(tree.fetch(tree.fetch(z).parent).parent)
			}
		} else { // other 3 cases
			y := tree.fetch(tree.fetch(tree.fetch(z).parent).parent).left // z uncle

			// first subcase
			if tree.fetch(y).isRed() {
				tree.fetch(tree.fetch(z).parent).setBlack()
				tree.fetch(y).setBlack()
				tree.fetch(tree.fetch(tree.fetch(z).parent).parent).setRed()
				z = tree.fetch(tree.fetch(z).parent).parent
			} else { // second and third subcases
				if z == tree.fetch(tree.fetch(z).parent).left { // second subcase, turning to third
					z = tree.fetch(z).parent
					tree.rightRotate(z)
				}

				// third case
				tree.fetch(tree.fetch(z).parent).setBlack()
				tree.fetch(tree.fetch(tree.fetch(z).parent).parent).setRed()
				tree.leftRotate(tree.fetch(tree.fetch(z).parent).parent)
			}
		}
	}

	tree.fetch(tree.meta.rootPtr).setBlack()
}

func (tree *RBTree) insert(z uint32) {
	y := tree.meta.nullPtr
	temp := tree.meta.rootPtr

	for temp != tree.meta.nullPtr {
		y = temp
		if bytes.Compare(tree.fetch(z).key, tree.fetch(temp).key) == -1 {
			temp = tree.fetch(temp).left
		} else {
			temp = tree.fetch(temp).right
		}
	}

	tree.fetch(z).dirty = true
	tree.fetch(z).parent = y
	if y == tree.meta.nullPtr {
		tree.meta.dirty = true
		tree.meta.rootPtr = z
	} else if bytes.Compare(tree.fetch(z).key, tree.fetch(y).key) == -1 {
		tree.fetch(y).dirty = true
		tree.fetch(y).left = z
	} else {
		tree.fetch(y).dirty = true
		tree.fetch(y).right = z
	}

	tree.fetch(z).left = tree.meta.nullPtr
	tree.fetch(z).right = tree.meta.nullPtr

	tree.fixInsert(z)

	tree.meta.dirty = true
	tree.meta.count++
}

func (tree *RBTree) leftRotate(x uint32) {
	y := tree.fetch(x).right

	tree.fetch(x).dirty = true
	tree.fetch(x).right = tree.fetch(y).left
	if tree.fetch(y).left != tree.meta.nullPtr {
		tree.fetch(tree.fetch(y).left).dirty = true
		tree.fetch(tree.fetch(y).left).parent = x
	}

	tree.fetch(y).dirty = true
	tree.fetch(y).parent = tree.fetch(x).parent

	if tree.fetch(x).parent == tree.meta.nullPtr { // x is root
		tree.meta.dirty = true
		tree.meta.rootPtr = y
	} else {
		tree.fetch(tree.fetch(x).parent).dirty = true
		if tree.fetch(tree.fetch(x).parent).left == x { // x is left child
			tree.fetch(tree.fetch(x).parent).left = y
		} else { // x is right child
			tree.fetch(tree.fetch(x).parent).right = y
		}
	}

	tree.fetch(y).left = x
	tree.fetch(x).parent = y
}

func (tree *RBTree) rightRotate(x uint32) {
	y := tree.fetch(x).left

	tree.fetch(x).dirty = true
	tree.fetch(x).left = tree.fetch(y).right
	if tree.fetch(y).right != tree.meta.nullPtr {
		tree.fetch(tree.fetch(y).right).dirty = true
		tree.fetch(tree.fetch(y).right).parent = x
	}

	tree.fetch(y).dirty = true
	tree.fetch(y).parent = tree.fetch(x).parent

	if tree.fetch(x).parent == tree.meta.nullPtr { // x is root
		tree.meta.dirty = true
		tree.meta.rootPtr = y
	} else {
		tree.fetch(tree.fetch(x).parent).dirty = true
		if tree.fetch(tree.fetch(x).parent).right == x { // x is right child
			tree.fetch(tree.fetch(x).parent).right = y
		} else { // x is left child
			tree.fetch(tree.fetch(x).parent).left = y
		}
	}

	tree.fetch(y).right = x
	tree.fetch(x).parent = y
}

func (tree *RBTree) pointer(rawPtr uint32) *pointer {
	return &pointer{
		pageId: rawPtr / uint32(tree.meta.pageSize),
		index:  (uint16(rawPtr) % tree.meta.pageSize) / tree.nodeSize,
	}
}

func (tree *RBTree) pointerRaw(ptr *pointer) uint32 {
	return ptr.pageId * uint32(tree.meta.pageSize) + uint32(ptr.index * tree.nodeSize)
}

func (tree *RBTree) page(id uint32) *page {
	return &page{
		dirty: true,
		id:    id,
		size:  tree.meta.pageSize,
		nodeSize: tree.meta.nodeKeySize + nodeFixedSize,
		nodeKeySize: tree.meta.nodeKeySize,
		nodes: make([]*node, tree.degree),
	}
}

func (tree *RBTree) fetch(rawPtr uint32) *node {
	if rawPtr == 0 {
		panic(ErrInvalidPointer)
	}

	ptr := tree.pointer(rawPtr)
	return tree.fetchPage(ptr.pageId).nodes[ptr.index]
}

func (tree *RBTree) fetchPage(id uint32) *page {
	if p, ok := tree.pages[id]; ok {
		return p
	}

	p := tree.page(id)
	if err := tree.pager.Unmarshal(uint64(id), p); err != nil {
		panic(errors.Wrap(err, "failed to unmarshal fetched page"))
	}

	p.dirty = false
	tree.pages[id] = p
	return p
}

func (tree *RBTree) alloc() (uint32, error) {
	topPtr := tree.pointer(tree.meta.top)

	if topPtr.index == 0 {
		var err error
		_, err = tree.pager.Alloc(1)
		if err != nil {
			return 0, errors.Wrap(err, "failed to alloc page")
		}
	}

	ptr := tree.meta.top
	tree.meta.dirty = true
	if topPtr.index == tree.degree - 1 {
		topPtr.pageId++
		topPtr.index = 0
	} else {
		topPtr.index++
	}
	tree.meta.top = tree.pointerRaw(topPtr)

	return ptr, nil
}

func (tree *RBTree) free(ptr uint32) error {
	lnPtr := tree.pointer(tree.meta.top)
	if lnPtr.index == 0 {
		lnPtr.pageId--
		lnPtr.index = tree.degree - 1
	} else {
		lnPtr.index--
	}
	lastNodePtr := tree.pointerRaw(lnPtr)

	if ptr != lastNodePtr {
		lastNode := tree.fetch(lastNodePtr)
		parent := tree.fetch(lastNode.parent)

		parent.dirty = true
		if lastNodePtr == parent.left {
			parent.left = ptr
		} else {
			parent.right = ptr
		}

		freedNode := tree.fetch(ptr)
		freedNode.dirty = true
		freedNode.flags = lastNode.flags
		freedNode.key = lastNode.key
		freedNode.left = lastNode.left
		freedNode.parent = lastNode.parent
		freedNode.right = lastNode.right
		freedNode.size = lastNode.size

		if freedNode.right != tree.meta.nullPtr {
			fr := tree.fetch(freedNode.right)
			fr.dirty = true
			fr.parent = ptr
		}
		
		if freedNode.left != tree.meta.nullPtr {
			fl := tree.fetch(freedNode.left)
			fl.dirty = true
			fl.parent = ptr
		}

		if lastNodePtr == tree.meta.rootPtr {
			tree.meta.rootPtr = ptr
		}
	}

	topPtr := tree.pointer(tree.meta.top)
	if lnPtr.pageId < topPtr.pageId {
		err := tree.pager.Free(1)
		if err != nil {
			return errors.Wrap(err, "failed to free last page")
		}
		delete(tree.pages, topPtr.pageId)
	}

	tree.meta.dirty = true
	tree.meta.top = lastNodePtr

	return nil
}

func (tree *RBTree) open(opts *Options) error {
	if tree.pager.Count() == 0 {
		return tree.init(opts)
	}

	if err := tree.pager.Unmarshal(0, tree.meta); err != nil {
		return errors.Wrap(err, "failed to unmarshal meta")
	}

	return nil
}

func (tree *RBTree) init(opts *Options) error {
	_, err := tree.pager.Alloc(1)
	if err != nil {
		return errors.Wrap(err, "failed to alloc first page for meta")
	}

	tree.meta = &metadata{
		dirty:    true,

		pageSize:    opts.PageSize,
		nodeKeySize: opts.KeySize,
		top:         uint32(opts.PageSize),
	}

	nullNode, err := tree.alloc()
	if err != nil {
		return errors.Wrap(err, "failed to alloc null node")
	}

	tree.fetch(nullNode).setBlack()

	tree.meta.dirty = true
	tree.meta.nullPtr = nullNode
	tree.meta.rootPtr = nullNode

	return nil
}

func (tree *RBTree) writeAll() error {
	if tree.pager.ReadOnly() {
		return nil
	}

	for _, p := range tree.pages {
		if !p.dirty {
			for _, n := range p.nodes {
				if n.dirty {
					p.dirty = true
					n.dirty = false
					break
				}
			}
		}

		if p.dirty {
			if err := tree.pager.Marshal(uint64(p.id), p); err != nil {
				return errors.Wrap(err, "failed to marshal dirty page")
			}
			p.dirty = false
		}
	}

	return errors.Wrap(tree.writeMeta(), "failed to write meta")
}

func (tree *RBTree) writeMeta() error {
	if tree.meta.dirty {
		err := tree.pager.Marshal(0, tree.meta)
		tree.meta.dirty = false
		return errors.Wrap(err, "failed to marshal dirty meta")
	}

	return nil
}
