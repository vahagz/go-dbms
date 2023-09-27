package freelist

import (
	"encoding/binary"
	"go-dbms/pkg/pager"
)

var bin = binary.BigEndian

func Open(fileName string, opts Options) (Freelist, error) {
	p, err := pager.Open(fileName, int(opts.freelistPageSize), false, 0664)
	if err != nil {
		return nil, err
	}

	fl := &freelist{
		targetPageSize: opts.targetPageSize,
		allocator:      opts.allocator,
		pager:          p,
		pages:          map[uint32]*page{},
	}

	return fl, fl.init()
}

type Allocator interface {
	Alloc(n int) (uint64, error)
}

type Freelist interface {
	Get(targetPageId uint64) uint16
	Set(targetPageId uint64, size uint16)
	Fit(size uint) (uint64, error)
}

type freelist struct {
	targetPageSize uint16
	allocator      Allocator
	pager          *pager.Pager
	pages          map[uint32]*page
}

func (fl *freelist) Get(pageId uint64) uint16 {
	return 0
}

func (fl *freelist) Set(pageId uint64, size uint16) {

}

func (fl *freelist) Fit(size uint) (uint64, error) {
	found := false
	pid := uint64(0)
	freeSpace := uint16(0)

	for id, fs := range fl.list {
		if uint(fs) >= size && (!found || fs < freeSpace) {
			found = true
			pid = id
			freeSpace = fs
		}
	}

	if found {
		return pid, nil
	}
	return fl.allocator.Alloc(1)
}

func (fl *freelist) init() error {

}
