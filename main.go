package main

import (
	"fmt"
	r "math/rand"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"go-dbms/config"
	"go-dbms/server"
	"go-dbms/services/auth"
	"go-dbms/services/executor"
	"go-dbms/services/parser"
)

var seed = time.Now().UnixMilli()
var rand = r.New(r.NewSource(seed))

func main() {
	// pw, _ := os.Getwd()

	// p := parser.New()
	// e, err := executor.New(path.Join(pw, "test/tables"))
	// fatalIfErr(err)

	// q, err := p.ParseQuery([]byte(`
	// 	SELECT id, firstname, lastname, COUNT() AS cnt, SUM(amount) AS sumAmount
	// 	FROM testtable
	// 	WHERE_INDEX id id >= 0 AND id <= 11
	// 	GROUP BY id, firstname, lastname;
	// `))
	// fatalIfErr(err)
	// fmt.Println(q)

	// r, err := e.Exec(q)
	// fatalIfErr(err)
	// fmt.Println(r.WriteTo(os.Stdout))

	// return

	pwd, _ := os.Getwd()
	as := auth.New()
	ps := parser.New()
	es, err := executor.New(path.Join(pwd, "test/tables"))
	fatalIfErr(err)

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

func fatalIfErr(val interface{}) {
	if val == nil {
		return
	}
	fmt.Println(val)
	os.Exit(1)
}

func fatalf(format string, values ...interface{}) {
	fmt.Printf(format, values...)
	os.Exit(1)
}
