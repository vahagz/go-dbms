package freelist

const PointerSize = 6

type Pointer struct {
	PageId uint32
	Index  uint16
}

func (p *Pointer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, PointerSize)
	
	bin.PutUint32(buf[0:4], p.PageId)
	bin.PutUint16(buf[4:6], p.Index)

	return buf, nil
}

func (p *Pointer) UnmarshalBinary(d []byte) error {
	p.PageId = bin.Uint32(d[0:4])
	p.Index = bin.Uint16(d[4:6])
	return nil
}
