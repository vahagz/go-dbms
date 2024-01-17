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

	"github.com/sirupsen/logrus"
)

const PROTOCOL = "tcp"

func Start(configs *config.ServerConfig, as *auth.AuthServiceT) error {
	url := fmt.Sprintf("%v:%v", configs.Host, configs.Port)
	addr, err := net.ResolveTCPAddr(PROTOCOL, url)
	if err != nil {
		logrus.Error(err)
		return errors.New("Unable to resolve IP: " + url)
	}

	listen, err := net.ListenTCP(PROTOCOL, addr)
	if err != nil {
		logrus.Error(err)
		return errors.New("Unable to listen addr: " + url)
	}

	logrus.Infof("Server started successfuly [%v]", url)

	defer listen.Close()
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			logrus.Errorf("Error while accepting tcp connection: %s", err)
		}

		err = conn.SetKeepAlive(true)
		if err != nil {
			logrus.Errorf("Unable to set keepalive: %s", err)
			conn.Close()
			continue
		}

		go handleConnection(connection.NewConnection(conn, 3, as))
	}
}

func handleConnection(c connection.Connection) {
	conn := c.GetConnection()
	logrus.Infof("client connected: %s", conn.RemoteAddr())
	defer func() {
		logrus.Infof("client disconnected: %s", conn.RemoteAddr())
		conn.Close()
	}()

	err := c.WaitAuth(30)
	if err != nil {
		logrus.Error("auth error: ", err)
		err = c.SendAuthError()
		if err != nil {
			logrus.Error("sendAuthError: ", err)
		}
		return
	}

	logrus.Info("client authed!")
	c.SendAuthSuccess()

	s := bufio.NewScanner(conn)
	s.Split(parser.QueryDivider)

	for s.Scan() {
		binary := s.Bytes()
		logrus.Infof("`%v`", string(binary))

		_, err := c.Send(binary)
		if err != nil {
			logrus.Error("Error while responding to client", err)
		}
	}
}
