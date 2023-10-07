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
		root:     nil,
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
	degree   uint16
	nodeSize uint16
}

func (tree *RBTree) Insert(k []byte) error {
	if len(k) != int(tree.meta.nodeKeySize) {
		return errors.Wrap(ErrInvalidKeySize, "insert key size missmatch")
	}

	n, err := tree.alloc(1)
	if err != nil {
		return errors.Wrap(err, "failed to alloc 1 node")
	}
	
	copy(n[0].key, k)

	err = tree.insert(n[0])
	if err != nil {
		return errors.Wrap(err, "failed to insert node")
	}
	return tree.writeAll()
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
			if curr.left == 0 {
				break
			}

			curr, err = tree.fetch(curr.left)
			if err != nil {
				return errors.Wrap(err, "failed to fetch node")
			}
		}

		curr = s.Pop();
		stop, err := scanFn(curr.key)
		if err != nil {
			return errors.Wrap(err, "error while scanning")
		} else if stop {
			return nil
		}

		if curr.right == 0 {
			curr = nil
		} else {
			curr, err = tree.fetch(curr.right)
			if err != nil {
				return errors.Wrap(err, "failed to fetch node")
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

func (tree *RBTree) Close() error {
	if tree.pager == nil {
		return nil
	}

	_ = tree.writeAll()
	err := tree.pager.Close()
	tree.pager = nil
	return errors.Wrap(err, "failed to close RBTree")
}

func (tree *RBTree) height() int {
	return 2 * int(math.Ceil(math.Log2(float64(tree.meta.count)))) + 1
}

func (tree *RBTree) fix(n *node) error {
	if n == tree.root {
		n.dirty = true
		n.color = NODE_COLOR_BLACK
		return nil
	}

	// node parent
	np, err := tree.fetch(n.parent)
	if err != nil {
		return errors.Wrap(err, "failed to fetch parent node")
	}

	if np.color == NODE_COLOR_BLACK {
		return nil
	}
	
	// node grandparent
	ng, err := tree.fetch(np.parent)
	if err != nil {
		return errors.Wrap(err, "failed to fetch grandparent node")
	}

	if ng.left == np.ptr.raw { // first 3 cases
		// node uncle
		var nu *node
		if ng.right != 0 {
			nu, err = tree.fetch(ng.right)
			if err != nil {
				return errors.Wrap(err, "failed to fetch uncle node")
			}
		}

		// first subcase
		if nu != nil && nu.color == NODE_COLOR_RED {
			np.dirty = true
			nu.dirty = true
			ng.dirty = true
			np.color = NODE_COLOR_BLACK
			nu.color = NODE_COLOR_BLACK
			ng.color = NODE_COLOR_RED
			return tree.fix(ng)
		}

		// second and third subcases
		// second subcase, turning to third
		if np.right == n.ptr.raw {
			ng, n, err = tree.leftRotate(np)
			if err != nil {
				return errors.Wrap(err, "failed to left rotate parent")
			}
		}

		// third case
		np.dirty = true
		ng.dirty = true
		np.color = NODE_COLOR_BLACK
		ng.color = NODE_COLOR_RED
		tree.rightRotate(ng)
		return nil
	}

	// other 3 cases (symmetric to previous 3 cases)
	// node uncle
	var nu *node
	if ng.left != 0 {
		nu, err = tree.fetch(ng.left)
		if err != nil {
			return errors.Wrap(err, "failed to fetch uncle node")
		}
	}

	// first subcase
	if nu != nil && nu.color == NODE_COLOR_RED {
		np.dirty = true
		nu.dirty = true
		ng.dirty = true
		np.color = NODE_COLOR_BLACK
		nu.color = NODE_COLOR_BLACK
		ng.color = NODE_COLOR_RED
		return tree.fix(ng)
	}

	// second and third subcases
	// second subcase, turning to third
	if np.left == n.ptr.raw {
		ng, n, err = tree.rightRotate(np)
		if err != nil {
			return errors.Wrap(err, "failed to right rotate parent")
		}
	}

	// third subcase
	np.dirty = true
	ng.dirty = true
	np.color = NODE_COLOR_BLACK
	ng.color = NODE_COLOR_RED
	tree.leftRotate(ng)
	return nil
}

func (tree *RBTree) insert(n *node) error {
	if tree.root == nil {
		n.dirty = true
		n.color = NODE_COLOR_BLACK
		tree.meta.dirty = true
		tree.meta.rootPtr = n.ptr.raw
		tree.meta.count++
		tree.root = n
		return nil
	}

	var err error
	var y *node
	subtree := tree.root
	for subtree != nil {
		y = subtree
		if bytes.Compare(n.key, subtree.key) == -1 {
			if subtree.left != 0 {
				subtree, err = tree.fetch(subtree.left)
			} else {
				subtree = nil
			}
		} else {
			if subtree.right != 0 {
				subtree, err = tree.fetch(subtree.right)
			} else {
				subtree = nil
			}
		}

		if err != nil {
			return errors.Wrap(err, "failed to fetch subtree")
		}
	}

	n.dirty = true
	n.parent = y.ptr.raw
	n.left = 0
	n.right = 0
	n.color = NODE_COLOR_RED
	if bytes.Compare(n.key, y.key) == -1 {
		y.left = n.ptr.raw
	} else {
		y.right = n.ptr.raw
	}

	err = tree.fix(n)
	if err != nil {
		return errors.Wrap(err, "failed to fix tree")
	}
	tree.meta.dirty = true
	tree.meta.count++
	return nil
}

func (tree *RBTree) leftRotate(x *node) (*node, *node, error) {
	if x.right == 0 {
		return nil, nil, errors.New("no right node in x to perform left rotation")
	}

	y, err := tree.fetch(x.right)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to fetch node")
	}

	x.dirty = true
	x.right = y.left
	if y.left != 0 {
		yl, err := tree.fetch(y.left)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch node")
		}

		yl.dirty = true
		yl.parent = x.ptr.raw
	}

	y.dirty = true
	y.parent = x.parent
	y.left = x.ptr.raw

	if x.parent != 0 {
		xp, err := tree.fetch(x.parent)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch node")
		}

		xp.dirty = true
		if xp.left == x.ptr.raw {
			xp.left = y.ptr.raw
		} else {
			xp.right = y.ptr.raw
		}
	} else {
		tree.meta.dirty = true
		tree.meta.rootPtr = y.ptr.raw
		tree.root = y
	}

	x.parent = y.ptr.raw
	return x, y, nil
}

func (tree *RBTree) rightRotate(y *node) (*node, *node, error) {
	if y.left == 0 {
		return nil, nil, errors.New("no left node in y to perform right rotation")
	}

	x, err := tree.fetch(y.left)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to fetch node")
	}

	y.dirty = true
	y.left = x.right
	if x.right != 0 {
		xr, err := tree.fetch(x.right)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch node")
		}

		xr.dirty = true
		xr.parent = y.ptr.raw
	}

	x.dirty = true
	x.parent = y.parent
	x.right = y.ptr.raw

	if y.parent != 0 {
		yp, err := tree.fetch(y.parent)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch node")
		}

		yp.dirty = true
		if yp.right == y.ptr.raw {
			yp.right = x.ptr.raw
		} else {
			yp.left = x.ptr.raw
		}
	} else {
		tree.meta.dirty = true
		tree.meta.rootPtr = x.ptr.raw
		tree.root = x
	}

	y.parent = x.ptr.raw
	return x, y, nil
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
	if rawPtr == 0 {
		return nil, ErrNilPtr
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

	if tree.meta.rootPtr != 0 {
		root, err := tree.fetch(tree.meta.rootPtr)
		if err != nil {
			return errors.Wrap(err, "failed to fetch root node")
		}
		tree.root = root
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
		rootPtr:     0,
	}

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

	return tree.writeMeta()
}

func (tree *RBTree) writeMeta() error {
	if tree.meta.dirty {
		err := tree.pager.Marshal(0, tree.meta)
		tree.meta.dirty = false
		return errors.Wrap(err, "failed to marshal dirty meta")
	}

	return nil
}
