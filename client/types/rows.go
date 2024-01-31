package types

import (
	"bytes"
	"encoding/json"
	"net"

	"go-dbms/client/util/pipe"
	"go-dbms/client/util/response"
)

type Rows struct {
	Conn *net.TCPConn
	Res  *response.Reader
	msg  []byte
}

func (rs *Rows) Close() error {
	return nil
}

func (rs *Rows) ColumnTypes() ([]*ColumnType, error) {
	return nil, nil
}

func (rs *Rows) Columns() ([]string, error) {
	return nil, nil
}

func (rs *Rows) Err() error {
	return nil
}

func (rs *Rows) Next() bool {
	var err error
	rs.msg, err = rs.Res.ReadLine()
	if err != nil {
		panic(err)
	}
	return !bytes.Equal(pipe.EOS, rs.msg)
}

func (rs *Rows) NextResultSet() bool {
	return false
}

func (rs *Rows) Scan(dest ...any) error {
	row := []json.RawMessage{}
	if err := json.Unmarshal(rs.msg, &row); err != nil {
		return err
	}

	for i, rm := range row {
		if err := json.Unmarshal(rm, dest[i]); err != nil {
			return err
		}
	}

	return nil
}