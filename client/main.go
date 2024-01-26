package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-dbms/services/executor"
	"go-dbms/util/response"
	"io"
	"net"
	"os"

	"github.com/pkg/errors"
)

const (
	HOST = "localhost"
	PORT = "8080"
	TYPE = "tcp"
)

type Client struct {
	conn *net.TCPConn
}

func (c *Client) Query(b []byte) (res []byte, err error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(b)))

	_, err = c.conn.Write(header)
	if err != nil {
		fmt.Println("Error while sending header: ", err)
		return nil, errors.New("Response error")
	}

	_, err = c.conn.Write(b)
	if err != nil {
		fmt.Println("Error while sending data: ", err)
		return nil, errors.New("Response error")
	}

	return readResponse(c.conn).Bytes(), nil
}

func readResponse(res io.Reader) *bytes.Buffer {
	buf := new(bytes.Buffer)
	rr := response.NewReader(res)

	for {
		msg, err := rr.ReadLine()
		if err != nil {
			fmt.Println("read error =>", err)
			return buf
		}
		
		if bytes.Compare(executor.EOS, msg) == 0 {
			break
		}

		fmt.Println(string(msg))
		_, err = buf.Write(msg)
		if err != nil {
			fmt.Println("write error =>", err)
			return buf
		}

		// _, err = buf.WriteRune('\n')
		// if err != nil {
		// 	fmt.Println("write error =>", err)
		// 	return buf
		// }
	}

	return buf
}

func main() {
	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)

	if err != nil {
		fmt.Println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP(TYPE, nil, tcpServer)
	if err != nil {
		fmt.Println("Dial failed:", err.Error())
		os.Exit(1)
	}

	err = conn.SetKeepAlive(true)
	if err != nil {
		fmt.Println("Unable to set keepalive - ", err)	
		os.Exit(1)
	}

	client := &Client{conn}
	var res []byte
	_ = res

	res, err = client.Query([]byte("username:password"))
	exitIfErr(errors.Wrap(err, "auth failed"))
	fmt.Printf("total bytes received: %v\n", len(res))

	// res, err = client.Query([]byte(`{
	// 	"type": "CREATE",
	// 	"target": "TABLE",
	// 	"name": "testtable",
	// 	"columns": [
	// 		{
	// 			"name": "id",
	// 			"type": 0,
	// 			"meta": {
	// 				"signed": false,
	// 				"bit_size": 4,
	// 				"auto_increment": {
	// 					"enabled": true
	// 				}
	// 			}
	// 		},
	// 		{
	// 			"name": "firstname",
	// 			"type": 2,
	// 			"meta": {
	// 				"cap": 32
	// 			}
	// 		},
	// 		{
	// 			"name": "lastname",
	// 			"type": 2,
	// 			"meta": {
	// 				"cap": 32
	// 			}
	// 		}
	// 	],
	// 	"indexes": [
	// 		{
	// 			"name": "id_1",
	// 			"columns": [ "id" ],
	// 			"primary": true,
	// 			"auto_increment": true
	// 		},
	// 		{
	// 			"name": "firstname_lastname_1",
	// 			"columns": [ "firstname", "lastname" ]
	// 		}
	// 	]
	// }`))
	// exitIfErr(errors.Wrap(err, "query failed failed"))
	// fmt.Printf("total bytes received: %v\n", len(res))

	// for i := 0; i < 4000; i++ {
	// 	res, err = client.Query([]byte(`{
	// 		"type": "INSERT",
	// 		"table": "testtable",
	// 		"columns": [ "firstname", "lastname" ],
	// 		"values": [
	// 			[ "Vahag", "Zargaryan" ],
	// 			[ "Ruben", "Manandyan" ],
	// 			[ "Sergey", "Zargaryan" ],
	// 			[ "Arman", "Sargsyan" ],
	// 			[ "Mery", "Voskanyan" ],
	// 			[ "David", "Harutyunyan" ],
	// 			[ "Alexader", "Bakunc" ],
	// 			[ "Hayk", "Vardanyan" ],
	// 			[ "Serob", "Gevorgyan" ],
	// 			[ "Gevorg", "Aznauryan" ]
	// 		]
	// 	}`))
	// 	exitIfErr(errors.Wrap(err, "query failed failed"))
	// }

	res, err = client.Query([]byte(`{
		"type": "SELECT",
		"table": "testtable",
		"columns": [ "id", "firstname", "lastname" ],
		"where_index": {
			"name": "id_1",
			"filter_start": {
				"operator": "<=",
				"value": {
					"id": 100
				}
			}
		}
	}`))
	exitIfErr(errors.Wrap(err, "query failed failed"))
	fmt.Printf("total bytes received: %v\n", len(res))

	// res, err = client.Query([]byte(`{
	// 	"type": "UPDATE",
	// 	"table": "testtable",
	// 	"values": {
	// 		"firstname": "dddddd"
	// 	},
	// 	"where_index": {
	// 		"name": "id_1",
	// 		"filter_start": {
	// 			"operator": ">=",
	// 			"value": {
	// 				"id": 4
	// 			}
	// 		},
	// 		"filter_end": {
	// 			"operator": "<=",
	// 			"value": {
	// 				"id": 6
	// 			}
	// 		}
	// 	},
	// 	"where": {
	// 		"or": [
	// 			{
	// 				"statement": {
	// 					"column": "firstname",
	// 					"operator": "=",
	// 					"value": "Arman"
	// 				}
	// 			},
	// 			{
	// 				"statement": {
	// 					"column": "lastname",
	// 					"operator": "=",
	// 					"value": "Harutyunyan"
	// 				}
	// 			}
	// 		]
	// 	}
	// }`))
	// exitIfErr(errors.Wrap(err, "query failed failed"))
	// fmt.Printf("total bytes received: %v\n", len(res))

	// res, err = client.Query([]byte(`{
	// 	"type": "DELETE",
	// 	"table": "testtable",
	// 	"where_index": {
	// 		"name": "id_1",
	// 		"filter_start": {
	// 			"operator": ">=",
	// 			"value": {
	// 				"id": 10
	// 			}
	// 		},
	// 		"filter_end": {
	// 			"operator": "<=",
	// 			"value": {
	// 				"id": 16
	// 			}
	// 		}
	// 	}
	// }`))
	// exitIfErr(errors.Wrap(err, "query failed failed"))
	// fmt.Printf("total bytes received: %v\n", len(res))

	conn.Close()
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(1)
	}
}
