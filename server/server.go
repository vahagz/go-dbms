package server

import (
	"errors"
	"fmt"
	"net"

	"go-dbms/config"
	"go-dbms/server/connection"
	"go-dbms/services/auth"
	"go-dbms/services/executor"
	"go-dbms/services/parser"
	"go-dbms/util/response"
)

const PROTOCOL = "tcp"

type Server struct {
	configs         *config.ServerConfig
	authService     *auth.AuthServiceT
	parserService   *parser.ParserServiceT
	executorService *executor.ExecutorServiceT

	listen *net.TCPListener
}

func New(
	configs *config.ServerConfig,
	authService *auth.AuthServiceT,
	parserService *parser.ParserServiceT,
	executorService *executor.ExecutorServiceT,
) (*Server, error) {
	var err error
	s := &Server{
		configs: configs,
		authService: authService,
		parserService: parserService,
		executorService: executorService,
	}

	url := fmt.Sprintf("%v:%v", configs.Host, configs.Port)
	addr, err := net.ResolveTCPAddr(PROTOCOL, url)
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("Unable to resolve IP: " + url)
	}

	s.listen, err = net.ListenTCP(PROTOCOL, addr)
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("Unable to listen addr: " + url)
	}

	fmt.Printf("Server started successfuly [%v]\n", url)

	return s, nil
}

func (s *Server) Start() <-chan error {
	ch := make(chan error)

	go func() {
		defer s.listen.Close()

		for {
			conn, err := s.listen.AcceptTCP()
			if err != nil {
				fmt.Printf("Error while accepting tcp connection: %s\n", err)
				conn.Close()
				continue
			}
	
			err = conn.SetKeepAlive(true)
			if err != nil {
				fmt.Printf("Unable to set keepalive: %s\n", err)
				conn.Close()
				continue
			}
	
			go s.handleConnection(&connection.Connection{Conn: conn})
		}
	}()

	return ch
}

func (s *Server) handleConnection(c *connection.Connection) {
	fmt.Printf("client connected: %s\n", c.Conn.RemoteAddr())
	defer func() {
		fmt.Printf("client disconnected: %s\n", c.Conn.RemoteAddr())
		c.Conn.Close()
	}()

	scanner := response.NewReader(c.Conn)

	err := c.Auth(scanner, s.configs.AuthTimeout, s.authService.ValidateCredentials)
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

	for {
		buf, err := scanner.ReadLine()
		if err != nil {
			_, err := c.Send([]byte(fmt.Sprintf("something went wrong: %v", err)))
			if err != nil {
				fmt.Println("unexpected error while responding:", err)
				break
			}
		}

		q, err := s.parserService.ParseQuery(buf)
		if err != nil {
			err = c.SendSyntaxError(err)
			if err != nil {
				fmt.Println("unexpected error while responding:", err)
			}
			break
		}

		r, err := s.executorService.Exec(q)
		if err != nil {
			err = c.SendError(err)
			if err != nil {
				fmt.Println("unexpected error while responding:", err)
			}
			break
		}

		totalBytes, err := c.Write(r)
		if err != nil {
			fmt.Println("unexpected error while responding:", err)
			break
		}

		fmt.Printf("total bytes sent: %v\n", totalBytes)
	}
}
