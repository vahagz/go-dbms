package connection

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"go-dbms/pkg/pipe"
	"go-dbms/util/response"

	"github.com/pkg/errors"
)

type Connection struct {
	Conn net.Conn
}

func (c *Connection) Auth(
	scanner *response.Reader,
	authTimeout uint,
	validate func(user, pass string) bool,
) error {
	errChan := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(authTimeout) * time.Second)
	defer cancel()

	go func() {
		creds, err := scanner.ReadLine()
		if err != nil {
			errChan <- errors.New("Error while reading credentials")
			return
		}

		credentialsArr := strings.Split(string(creds), ":")
		if len(credentialsArr) != 2 {
			errChan <- errors.New("Invalid credentials")
			return
		}

		if !validate(credentialsArr[0], credentialsArr[1]) {
			errChan <- errors.New("Auth error: invalid username/password")
			return
		}
		errChan <- nil
	}()

	select {
	case <-ctx.Done():
		fmt.Println("Auth context canceled: ", ctx.Err())
		return errors.New("Auth timeout")
	case err := <-errChan:
		return err
	}
}

func (c *Connection) Send(blob []byte) (int, error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(blob)))

	hn, err := c.Conn.Write(header)
	if err != nil {
		fmt.Println("Error while sending header: ", err)
		err = errors.New("Response error")
	} else {
		bn := 0
		bn, err = c.Conn.Write(blob)
		if err != nil {
			fmt.Println("Error while sending data: ", err)
			err = errors.New("Response error")
		} else {
			return hn + bn, err
		}
	}

	return 0, err
}

func (c *Connection) EOS() error {
	_, err := c.Send(pipe.EOS)
	return err
}

func (c *Connection) SendAuthError() error {
	_, err := c.Send([]byte("Auth failed, invalid username/password"))
	if err == nil {
		err = c.EOS()
	}
	return err
}

func (c *Connection) SendAuthSuccess() error {
	_, err := c.Send([]byte("Auth succeed"))
	if err == nil {
		err = c.EOS()
	}
	return err
}

func (c *Connection) SendSyntaxError(err error) error {
	_, e := c.Send([]byte(fmt.Sprintf("Syntax error: %v", err)))
	if err == nil {
		err = c.EOS()
	}
	return e
}

func (c *Connection) SendError(err error) error {
	_, e := c.Send([]byte(fmt.Sprintf("Error: %v", err)))
	if err == nil {
		err = c.EOS()
	}
	return e
}
