package main

import (
	"fmt"
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

func main() {
	pwd, _ := os.Getwd()
	as := auth.New()
	ps := parser.New()
	es, err := executor.New(path.Join(pwd, "test/tables"))
	fatalIfErr(err)

	defer es.Close()

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
