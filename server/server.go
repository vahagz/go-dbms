package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"go-dbms/config"
	"go-dbms/parser"
	"go-dbms/services"
	"go-dbms/util/logger"
)

const PROTOCOL = "tcp"

func Start(configs *config.ServerConfig, services *services.Services) error {
	url := fmt.Sprintf("%v:%v", configs.Host, configs.Port)
	addr, err := net.ResolveTCPAddr(PROTOCOL, url)
	if err != nil {
		logger.L.Error(err)
		return errors.New("Unable to resolve IP: " + url)
	}

	listen, err := net.ListenTCP(PROTOCOL, addr)
	if err != nil {
		logger.L.Error(err)
		return errors.New("Unable to listen addr: " + url)
	}

	logger.L.Infof("Server started successfuly [%v]", url)

	defer listen.Close()
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			logger.L.Error("Error while accepting tcp connection: %s", err)
		}
		
		err = conn.SetKeepAlive(true)
		if err != nil {
			logger.L.Errorf("Unable to set keepalive: %s", err)
			conn.Close()
			continue
		}

		go handleConnection(NewConnection(conn, 3, services.AuthService))
	}
}

func handleConnection(c Connection) {
	conn := c.GetConnection()
	logger.L.Infof("client connected: %s", conn.RemoteAddr())
	defer func() {
		logger.L.Infof("client disconnected: %s", conn.RemoteAddr())
		conn.Close()
	}()

	err := c.WaitAuth(30)
	if err != nil {
		logger.L.Error("auth error: ", err)
		err = c.SendAuthError()
		if err != nil {
			logger.L.Error("sendAuthError: ", err)
		}
		return
	}

	logger.L.Info("client authed!")
	c.SendAuthSuccess()

	s := bufio.NewScanner(conn)
	s.Split(parser.QueryDivider)

	for s.Scan() {
		binary := s.Bytes()
		logger.L.Infof("`%v`", string(binary))

		_, err := c.Send(binary)
		if err != nil {
			logger.L.Error("Error while responding to client", err)
		}
	}
}
