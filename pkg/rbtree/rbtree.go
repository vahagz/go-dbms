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
	root     *node            // current root node
	null     *node            // nil leaf node
	degree   uint16
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

	n, err := tree.alloc(1)
	if err != nil {
		return errors.Wrap(err, "failed to alloc 1 node")
	}
	
	copy(n[0].key, k)
	return errors.Wrap(tree.insert(n[0]), "failed to insert node")
}

func (tree *RBTree) Scan(t *node, scanFn func(key []byte) (bool, error)) error {
	if tree.root == nil {
		return nil
	}
	if t == nil {
		t = tree.root
	}

	var err error
	s := stack.New[*node](tree.height())
	curr := t
	for curr != nil || s.Size() > 0 {
		for curr != nil {
			s.Push(curr)
			if curr.left == tree.meta.nullPtr {
				break
			}

			curr, err = tree.fetch(curr.left)
			if err != nil {
				return errors.Wrap(err, ErrNodeFetch.Error())
			}
		}

		curr = s.Pop();
		stop, err := scanFn(curr.key)
		if err != nil {
			return errors.Wrap(err, "error while scanning")
		} else if stop {
			return nil
		}

		if curr.right == tree.meta.nullPtr {
			curr = nil
		} else {
			curr, err = tree.fetch(curr.right)
			if err != nil {
				return errors.Wrap(err, ErrNodeFetch.Error())
			}
		}
	}

	return nil
}

func (tree *RBTree) Print() error {
	count := tree.pager.Count()
	for i := uint32(1); i < uint32(count); i++ {
		p := tree.page(i)
		if err := tree.pager.Unmarshal(uint64(p.id), p); err != nil {
			return errors.Wrap(err, "failed to unmarshal page")
		}
		nodes := p.nodes[:tree.meta.count]
		fmt.Println(nodes)
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

// func (tree *RBTree) fixDelete(n *node) error {

// }

// func (tree *RBTree) delete(n *node) error {

// }

func (tree *RBTree) transplant(u, v *node) error {
	if u.parent == 0 { // u is root
		tree.meta.dirty = true
		tree.meta.rootPtr = v.ptr.raw
		tree.root = v
	} else {
		up, err := tree.fetch(u.parent)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}

		if u.ptr.raw == up.left { // u is left child
			up.left = v.ptr.raw
		} else { // u is right child
			up.right = v.ptr.raw
		}
	}

	v.parent = u.parent
	return nil
}

func (tree *RBTree) height() int {
	return 2 * int(math.Ceil(math.Log2(float64(tree.meta.count)))) + 1
}

func (tree *RBTree) fixInsert(z *node) error {
	for {
		// z parent
		zp, err := tree.fetch(z.parent)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}
	
		if zp.isBlack() {
			break
		}
		
		// z grandparent
		zg, err := tree.fetch(zp.parent)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}
	
		if zp.ptr.raw == zg.left { // first 3 cases
			// z uncle
			y, err := tree.fetch(zg.right)
			if err != nil {
				return errors.Wrap(err, ErrNodeFetch.Error())
			}
	
			// first subcase
			if y.isRed() {
				zp.dirty = true
				y.dirty = true
				zg.dirty = true
				zp.setBlack()
				y.setBlack()
				zg.setRed()
				z = zg
				continue
			}

			// second and third subcases
			if z.ptr.raw == zp.right { // second subcase, turning to third
				zpPtr := zp.ptr.raw
				zgPtr := zg.ptr.raw
				if err = tree.leftRotate(zp); err != nil {
					return err
				}

				zp, err = tree.fetch(zpPtr)
				if err != nil {
					return errors.Wrap(err, ErrNodeFetch.Error())
				}
				zg, err = tree.fetch(zgPtr)
				if err != nil {
					return errors.Wrap(err, ErrNodeFetch.Error())
				}
			}

			// third case
			zp.dirty = true
			zg.dirty = true
			zp.setBlack()
			zg.setRed()
			tree.rightRotate(zg)
			return nil
		} else { // other 3 cases
			// z uncle
			y, err := tree.fetch(zg.left)
			if err != nil {
				return errors.Wrap(err, ErrNodeFetch.Error())
			}
		
			// first subcase
			if y.isRed() {
				zp.dirty = true
				y.dirty = true
				zg.dirty = true
				zp.setBlack()
				y.setBlack()
				zg.setRed()
				z = zg
				continue
			}
		
			// second and third subcases
			if z.ptr.raw == zp.left { // second subcase, turning to third
				zpPtr := zp.ptr.raw
				zgPtr := zg.ptr.raw
				if err = tree.rightRotate(zp); err != nil {
					return err
				}

				zp, err = tree.fetch(zpPtr)
				if err != nil {
					return errors.Wrap(err, ErrNodeFetch.Error())
				}
				zg, err = tree.fetch(zgPtr)
				if err != nil {
					return errors.Wrap(err, ErrNodeFetch.Error())
				}
			}
		
			// third case
			zp.dirty = true
			zg.dirty = true
			zp.setBlack()
			zg.setRed()
			tree.leftRotate(zg)
			return nil
		}
	}

	tree.root.dirty = true
	tree.root.setBlack()
	return nil
}

func (tree *RBTree) insert(n *node) error {
	var err error
	y := tree.null
	temp := tree.root

	for temp.ptr.raw != tree.meta.nullPtr {
		y = temp
		if bytes.Compare(n.key, temp.key) == -1 {
			temp, err = tree.fetch(temp.left)
		} else {
			temp, err = tree.fetch(temp.right)
		}
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}
	}

	n.dirty = true
	n.parent = y.ptr.raw
	if y.ptr.raw == tree.meta.nullPtr {
		tree.meta.dirty = true
		tree.meta.rootPtr = n.ptr.raw
		tree.root = n
	} else if bytes.Compare(n.key, y.key) == -1 {
		y.left = n.ptr.raw
	} else {
		y.right = n.ptr.raw
	}

	n.left = tree.meta.nullPtr
	n.right = tree.meta.nullPtr
	n.setRed()

	err = tree.fixInsert(n)
	if err != nil {
		return errors.Wrap(err, "failed to fix tree")
	}

	tree.meta.dirty = true
	tree.meta.count++
	return nil
}

func (tree *RBTree) leftRotate(x *node) error {
	if x.right == 0 {
		return errors.New("no right node in x to perform left rotation")
	}

	y, err := tree.fetch(x.right)
	if err != nil {
		return errors.Wrap(err, ErrNodeFetch.Error())
	}

	x.dirty = true
	x.right = y.left
	if y.left != 0 {
		yl, err := tree.fetch(y.left)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}

		yl.dirty = true
		yl.parent = x.ptr.raw
	}

	y.dirty = true
	y.parent = x.parent

	if x.parent == tree.meta.nullPtr { // x is root
		tree.meta.dirty = true
		tree.meta.rootPtr = y.ptr.raw
		tree.root = y
	} else {
		xp, err := tree.fetch(x.parent)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}

		xp.dirty = true
		if xp.left == x.ptr.raw { // x is left child
			xp.left = y.ptr.raw
		} else { // x is right child
			xp.right = y.ptr.raw
		}
	}

	y.left = x.ptr.raw
	x.parent = y.ptr.raw
	return nil
}

func (tree *RBTree) rightRotate(y *node) error {
	if y.left == 0 {
		return errors.New("no left node in y to perform right rotation")
	}

	x, err := tree.fetch(y.left)
	if err != nil {
		return errors.Wrap(err, ErrNodeFetch.Error())
	}

	y.dirty = true
	y.left = x.right
	if x.right != 0 {
		xr, err := tree.fetch(x.right)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}

		xr.dirty = true
		xr.parent = y.ptr.raw
	}

	x.dirty = true
	x.parent = y.parent

	if y.parent == tree.meta.nullPtr { // y is root
		tree.meta.dirty = true
		tree.meta.rootPtr = x.ptr.raw
		tree.root = x
	} else {
		yp, err := tree.fetch(y.parent)
		if err != nil {
			return errors.Wrap(err, ErrNodeFetch.Error())
		}

		yp.dirty = true
		if yp.right == y.ptr.raw {
			yp.right = x.ptr.raw // y is right child
		} else { // y is left child
			yp.left = x.ptr.raw
		}
	}

	x.right = y.ptr.raw
	y.parent = x.ptr.raw
	return nil
}

func (tree *RBTree) pointer(rawPtr uint32) *pointer {
	return &pointer{
		raw:    rawPtr,
		pageId: rawPtr / uint32(tree.meta.pageSize),
		index:  (uint16(rawPtr) % tree.meta.pageSize) / tree.nodeSize,
	}
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

func (tree *RBTree) fetch(rawPtr uint32) (*node, error) {
	if rawPtr == tree.meta.nullPtr && tree.null != nil {
		return tree.null, nil
	}

	ptr := tree.pointer(rawPtr)

	if p, ok := tree.pages[ptr.pageId]; ok {
		return p.nodes[ptr.index], nil
	}

	p := tree.page(ptr.pageId)
	if err := tree.pager.Unmarshal(uint64(ptr.pageId), p); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal fetched page")
	}

	p.dirty = false
	tree.pages[ptr.pageId] = p
	return p.nodes[ptr.index], nil
}

func (tree *RBTree) alloc(n int) ([]*node, error) {
	topPtr := tree.pointer(tree.meta.top)
	requestedLastPtr := tree.pointer(tree.meta.top + uint32((n - 1) * int(tree.nodeSize)))

	if requestedLastPtr.pageId > topPtr.pageId || topPtr.index == 0 {
		var err error
		_, err = tree.pager.Alloc(int(requestedLastPtr.pageId - topPtr.pageId + 1))
		if err != nil {
			return nil, errors.Wrap(err, "failed to alloc page")
		}
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		n, err := tree.fetch(tree.meta.top + uint32(i * int(tree.nodeSize)))
		if err != nil {
			return nil, errors.Wrap(err, "failed to fetch allocated page")
		}
		nodes[i] = n
	}

	tree.meta.top += uint32(n * int(tree.nodeSize))

	return nodes, nil
}

func (tree *RBTree) open(opts *Options) error {
	if tree.pager.Count() == 0 {
		return tree.init(opts)
	}

	if err := tree.pager.Unmarshal(0, tree.meta); err != nil {
		return errors.Wrap(err, "failed to unmarshal meta")
	}

	root, err := tree.fetch(tree.meta.rootPtr)
	if err != nil {
		return errors.Wrap(err, "failed to fetch root node")
	}
	tree.root = root
	
	null, err := tree.fetch(tree.meta.nullPtr)
	if err != nil {
		return errors.Wrap(err, "failed to fetch root node")
	}
	tree.null = null

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

	n, err := tree.alloc(1)
	if err != nil {
		return errors.Wrap(err, "failed to alloc null node")
	}

	nullNode := n[0]
	nullNode.dirty = true
	nullNode.setBlack()
	tree.null = nullNode
	tree.root = nullNode

	tree.meta.dirty = true
	tree.meta.nullPtr = nullNode.ptr.raw
	tree.meta.rootPtr = nullNode.ptr.raw

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
