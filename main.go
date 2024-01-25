package main

import (
	"bytes"
	"fmt"
	"io"
	r "math/rand"
	"os"
	"path"
	"time"

	"go-dbms/pkg/column"
	"go-dbms/pkg/types"
	"go-dbms/services/executor"
	"go-dbms/services/parser"
	"go-dbms/util/response"
)

var seed = time.Now().UnixMilli()
// var seed int64 = 1704977223
var rand = r.New(r.NewSource(seed))

// func main() {
// 	configs := config.New()
// 	svcs := services.New()
// 	err := server.Start(configs.ServerConfig, svcs)
// 	fmt.Printf(err)
// }




func main() {
	pwd, _ := os.Getwd()
	ps := parser.New()
	es, err := executor.New(path.Join(pwd, "test/tables"))
	if err != nil {
		fatal(err)
	}

	defer func() {
		if err := es.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	q, err := ps.ParseQuery([]byte(`{
		"type": "CREATE",
		"target": "TABLE",
		"name": "testtable",
		"columns": [
			{
				"name": "id",
				"type": 0,
				"meta": {
					"signed": false,
					"bit_size": 4,
					"auto_increment": {
						"enabled": true
					}
				}
			},
			{
				"name": "firstname",
				"type": 2,
				"meta": {
					"cap": 32
				}
			},
			{
				"name": "lastname",
				"type": 2,
				"meta": {
					"cap": 32
				}
			}
		],
		"indexes": [
			{
				"name": "id_1",
				"columns": [ "id" ],
				"primary": true,
				"auto_increment": true
			},
			{
				"name": "firstname_lastname_1",
				"columns": [ "firstname", "lastname" ]
			}
		]
	}`))
	if err != nil {
		fatal(err)
	}
	res, err := es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)
	

	q, err = ps.ParseQuery([]byte(`{
		"type": "INSERT",
		"table": "testtable",
		"columns": [ "firstname", "lastname" ],
		"values": [
			[ "Vahag", "Zargaryan" ],
			[ "Ruben", "Manandyan" ],
			[ "Sergey", "Zargaryan" ],
			[ "Arman", "Sargsyan" ],
			[ "Mery", "Voskanyan" ],
			[ "David", "Harutyunyan" ],
			[ "Alexader", "Bakunc" ]
		]
	}`))
	if err != nil {
		fatal(err)
	}

	res, err = es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)


	q, err = ps.ParseQuery([]byte(`{
		"type": "SELECT",
		"table": "testtable",
		"columns": [ "id", "firstname", "lastname" ]
	}`))
	if err != nil {
		fatal(err)
	}

	res, err = es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)


	q, err = ps.ParseQuery([]byte(`{
		"type": "UPDATE",
		"table": "testtable",
		"values": {
			"firstname": "dddddd"
		},
		"where_index": {
			"name": "id_1",
			"filter_start": {
				"operator": ">=",
				"value": {
					"id": 4
				}
			},
			"filter_end": {
				"operator": "<=",
				"value": {
					"id": 6
				}
			}
		},
		"where": {
			"or": [
				{
					"statement": {
						"column": "firstname",
						"operator": "=",
						"value": "Arman"
					}
				},
				{
					"statement": {
						"column": "lastname",
						"operator": "=",
						"value": "Harutyunyan"
					}
				}
			]
		}
	}`))
	if err != nil {
		fatal(err)
	}

	res, err = es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)


	q, err = ps.ParseQuery([]byte(`{
		"type": "SELECT",
		"table": "testtable",
		"columns": [ "id", "firstname", "lastname" ]
	}`))
	if err != nil {
		fatal(err)
	}

	res, err = es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)

	
	q, err = ps.ParseQuery([]byte(`{
		"type": "DELETE",
		"table": "testtable"
	}`))
	if err != nil {
		fatal(err)
	}

	res, err = es.Exec(q)
	if err != nil {
		fatal(err)
	}
	printResponse(res)
}

func fatal(val interface{}) {
	fmt.Println(val)
	os.Exit(1)
}

func fatalf(format string, values ...interface{}) {
	fmt.Printf(format, values...)
	os.Exit(1)
}

func sprintData(columns []*column.Column, data []map[string]types.DataType) string {
	buf := bytes.Buffer{}
	for _, d := range data {
		for _, col := range columns {
			val := fmt.Sprintf("%v", d[col.Name].Value())
			buf.WriteByte('\'')
			buf.Write([]byte(col.Name))
			buf.Write([]byte("' -> '"))
			buf.Write([]byte(val))
			buf.WriteString("', ")
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func printData(columns []*column.Column, data []map[string]types.DataType) {
	fmt.Print(sprintData(columns, data))
}

func randomString(length int) string {
	bytes := make([]byte, 0, length)
	for i := 0; i < length; i++ {
		bytes = append(bytes, byte('a' + rand.Intn(int('z') - int('a'))))
	}
	return string(bytes)
}

func printResponse(res io.Reader) {
	rr := response.NewReader(res)
	for {
		msg, err := rr.ReadLine()
		fmt.Printf("%v '%s'\n", len(msg), string(msg))
		if err != nil {
			fmt.Println("error =>", err)
			break
		}
	}
}
