package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"go-dbms/config"
	"go-dbms/parser"
	"go-dbms/server/connection"
	"go-dbms/services/auth"
)

const PROTOCOL = "tcp"

func Start(configs *config.ServerConfig, as *auth.AuthServiceT) error {
	url := fmt.Sprintf("%v:%v", configs.Host, configs.Port)
	addr, err := net.ResolveTCPAddr(PROTOCOL, url)
	if err != nil {
		fmt.Println(err)
		return errors.New("Unable to resolve IP: " + url)
	}

	listen, err := net.ListenTCP(PROTOCOL, addr)
	if err != nil {
		fmt.Println(err)
		return errors.New("Unable to listen addr: " + url)
	}

	fmt.Printf("Server started successfuly [%v]\n", url)

	defer listen.Close()
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			fmt.Printf("Error while accepting tcp connection: %s\n", err)
		}

		err = conn.SetKeepAlive(true)
		if err != nil {
			fmt.Printf("Unable to set keepalive: %s\n", err)
			conn.Close()
			continue
		}

		go handleConnection(connection.NewConnection(conn, 3, as))
	}
}

func handleConnection(c connection.Connection) {
	conn := c.GetConnection()
	fmt.Printf("client connected: %s\n", conn.RemoteAddr())
	defer func() {
		fmt.Printf("client disconnected: %s\n", conn.RemoteAddr())
		conn.Close()
	}()

	err := c.WaitAuth(30)
	if err != nil {
		fmt.Println("auth error: ", err)
		err = c.SendAuthError()
		if err != nil {
			fmt.Println("sendAuthError: ", err)
		}
		return
	}

	fmt.Println("client authed!")
	c.SendAuthSuccess()

	s := bufio.NewScanner(conn)
	s.Split(parser.QueryDivider)

	for s.Scan() {
		binary := s.Bytes()
		fmt.Printf("`%v`\n", string(binary))

		_, err := c.Send(binary)
		if err != nil {
			fmt.Println("Error while responding to client", err)
		}
	}
}
