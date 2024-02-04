package main

import (
	"bytes"
	"fmt"
	"io"
	r "math/rand"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"go-dbms/config"
	"go-dbms/pkg/column"
	"go-dbms/pkg/types"
	"go-dbms/server"
	"go-dbms/services/auth"
	"go-dbms/services/executor"
	"go-dbms/services/parser"
	"go-dbms/util/response"
)

var seed = time.Now().UnixMilli()
var rand = r.New(r.NewSource(seed))

func main() {
	pwd, _ := os.Getwd()
	as := auth.New()
	ps := parser.New()
	es, err := executor.New(path.Join(pwd, "test/tables"))
	if err != nil {
		fatal(err)
	}

	defer func() {
		if err := es.Close(); err != nil {
			fmt.Println("error on gracefully stopping:", err)
		}
	}()

	configs := config.New()
	s, err := server.New(configs.ServerConfig, as, ps, es)
	if err != nil {
		fmt.Println("error while initializing server:", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err = <-s.Start():
		fmt.Println("App crushed:", err)
	case q := <-quit:
		fmt.Printf("\n%s signal received, stopping gracefully...\n", q.String())
	}
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
