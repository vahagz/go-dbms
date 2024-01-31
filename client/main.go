package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"go-dbms/client/types"
	"go-dbms/client/util/response"

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

func (c *Client) Query(b []byte) (*types.Rows, error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(b)))

	if _, err := c.conn.Write(header); err != nil {
		fmt.Println("Error while sending header: ", err)
		return nil, errors.New("Response error")
	}

	if _, err := c.conn.Write(b); err != nil {
		fmt.Println("Error while sending data: ", err)
		return nil, errors.New("Response error")
	}

	return &types.Rows{Conn: c.conn, Res: response.NewReader(c.conn)}, nil
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
	var rows *types.Rows
	_ = rows

	rows, err = client.Query([]byte("username:password"))
	exitIfErr(errors.Wrap(err, "auth failed"))
	var msg string
	for rows.Next() {
		rows.Scan(&msg)
		fmt.Printf("[auth] bytes received: %v\n", len(msg))
	}

	// t := time.Now()
	// rows, err = client.Query([]byte(`{
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
	// for rows.Next() {
	// 	fmt.Printf("[create] %v\n", time.Since(t))
	// }

	// t = time.Now()
	// for i := 0; i < 4000; i++ {
	// 	rows, err = client.Query([]byte(`{
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
	// 	for rows.Next() {  }
	// }
	// fmt.Printf("[insert] %v\n", time.Since(t))

	t := time.Now()
	rows, err = client.Query([]byte(`
		SELECT id, firstname, lastname
		FROM testtable
		WHERE_INDEX id_1 id >= 100 AND id < 800;
	`))
	exitIfErr(errors.Wrap(err, "query failed"))

	for rows.Next() {
		var (
			id int
			firstname, lastname string
		)
		if err := rows.Scan(&id, &firstname, &lastname); err != nil {
			exitIfErr(errors.Wrap(err, "scan failed"))
		}

		fmt.Println(id, firstname, lastname)
	}
	fmt.Printf("[select] %v\n", time.Since(t))

	// t = time.Now()
	// rows, err = client.Query([]byte(`{
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
	// for rows.Next() {  }
	// fmt.Printf("[update] %v\n", time.Since(t))

	// t = time.Now()
	// rows, err = client.Query([]byte(`{
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
	// for rows.Next() {  }
	// fmt.Printf("[delete] %v\n", time.Since(t))

	conn.Close()
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(1)
	}
}
