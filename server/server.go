package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"text/scanner"
	"time"

	"go-dbms/config"
	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/server/connection"
	"go-dbms/services/auth"
	"go-dbms/services/executor"
	"go-dbms/services/parser"
	"go-dbms/services/parser/query"
	"go-dbms/util/helpers"
	"go-dbms/util/response"

	"github.com/pkg/errors"
)

const PROTOCOL = "tcp"

type Server struct {
	configs         *config.ServerConfig
	authService     *auth.AuthServiceT
	parserService   query.Parser
	executorService *executor.ExecutorService

	listen *net.TCPListener
}

func New(
	configs *config.ServerConfig,
	authService *auth.AuthServiceT,
	parserService *parser.ParserServiceT,
	executorService *executor.ExecutorService,
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

	req := response.NewReader(c.Conn)

	err := c.Auth(req, s.configs.AuthTimeout, s.authService.ValidateCredentials)
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
		buf, err := req.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}

			_, err := c.Send([]byte(fmt.Sprintf("something went wrong: %v", err)))
			if err != nil {
				fmt.Println("[ReadQuery] unexpected error while responding:", err)
				break
			}
		}

		start := time.Now()
		sc := &scanner.Scanner{}
		sc.Init(bytes.NewReader(buf))
		sc.Scan()
		q, err := s.parserService.ParseQuery(sc)
		if err != nil {
			err = c.SendSyntaxError(err)
			if err != nil {
				fmt.Println("[Parse] unexpected error while responding:", err)
			}
			break
		}

		r, pr, err := s.executorService.Exec(q)
		if err != nil {
			fmt.Println("error while executing =>", err)
			err = c.SendError(err)
			if err != nil {
				fmt.Println("[Exec] unexpected error while responding:", err)
			}
			break
		}

		p := pipe.NewPipe(nil)
		go func ()  {
			if r != nil {
				record := make(types.DataSeq, len(pr.Iterator()))
				for row, ok := r.Pop(); ok; row, ok = r.Pop() {
					r.Continue(true)

					for i, p := range pr.Iterator() {
						record[i] = row[p.Alias]
					}

					if _, err := p.Write(helpers.MustVal(json.Marshal(record))); err != nil {			
						panic(errors.Wrap(err, "failed to push marshaled record"))
					}
				}
			}
			p.Write(pipe.EOS)
		}()

		totalBytes, err := p.WriteTo(c.Conn)
		if err != nil {
			fmt.Println("[Write] unexpected error while responding:", err)
			break
		}

		fmt.Printf("Server sent %d bytes\n", totalBytes)
		fmt.Printf("Duration %v\n", time.Since(start))
	}
}
