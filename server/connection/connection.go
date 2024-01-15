package connection

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"go-dbms/services/auth"
	"go-dbms/util/helpers"

	"github.com/sirupsen/logrus"
)

type ConnectionT struct {
	conn        net.Conn;
	as          auth.AuthService;
}

type Connection interface {
	GetConnection() net.Conn;
	WaitAuth(authTimeout int) error;
	Send(blob []byte) (int, error);
	SendAuthError() error;
	SendAuthSuccess() error;
}

func NewConnection(conn net.Conn, authTimeoutSec uint, as auth.AuthService) Connection {
	return &ConnectionT{
		conn:        conn,
		as:          as,
	}
}

func (c *ConnectionT) GetConnection() net.Conn {
	return c.conn
}

func (c *ConnectionT) WaitAuth(authTimeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(authTimeout) * time.Second)
	defer cancel()
	ch := make(chan error, 1)

	go func() {
		credentials, readErr := bufio.NewReader(c.conn).ReadString(';')
		if readErr != nil {
			logrus.Warn(readErr)
			ch<- errors.New("Error while reading credentials")
			return
		}

		credentialsArr := strings.Split(helpers.TrimSuffix(credentials, ";"), ":")
		if len(credentialsArr) < 2 {
			ch<- errors.New("Invalid credentials")
			return
		}

		if !c.as.ValidateCredentials(credentialsArr[0], credentialsArr[1]) {
			ch<- errors.New("Auth error: invalid username/password")
			return
		}

		ch<- nil
	}()

	select {
	case <-ctx.Done():
		logrus.Error("Auth context canceled: ", ctx.Err())
		return errors.New("Auth timeout")
	case err := <-ch:
		return err
	}
}

func (c *ConnectionT) Send(blob []byte) (int, error) {
	blob = append(blob, '\x00')
	n, err := c.conn.Write(blob)
	if err != nil {
		logrus.Error("Error while sending data: ", err)
		err = errors.New("Response error")
	}
	
	logrus.Infof("Server sent %v bytes", n)
	return n, err
}

func (c *ConnectionT) SendAuthError() error {
	_, err := c.Send([]byte("Auth failed, invalid username/password"))
	return err
}

func (c *ConnectionT) SendAuthSuccess() error {
	_, err := c.Send([]byte("auth_success"))
	return err
}
