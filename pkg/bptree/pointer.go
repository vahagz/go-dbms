package bptree

type Pointer struct {
	FreeSpace uint16
	PageId    uint64
}

func (p *Pointer) MarshalKey() ([][]byte, error) {
	key := make([][]byte, 2)
	bytes, err := p.MarshalBinary()
	if err != nil {
		return nil, err
	}

	key[0] = bytes[0:2]
	key[1] = bytes[2:10]
	return key, nil
}

func (p *Pointer) UnmarshalKey(d [][]byte) error {
	buf := make([]byte, 10)
	copy(buf[0:2], d[0])
	copy(buf[2:10], d[1])
	return p.UnmarshalBinary(buf)
}

func (p *Pointer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 10)
	bin.PutUint16(buf[0:2], p.FreeSpace)
	bin.PutUint64(buf[2:10], p.PageId)
	return buf, nil
}

func (p *Pointer) UnmarshalBinary(d []byte) error {
	p.FreeSpace = bin.Uint16(d[0:2])
	p.PageId = bin.Uint64(d[2:10])
	return nil
}
