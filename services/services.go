package services

import "go-dbms/services/auth"

type Services struct {
	AuthService auth.AuthService
}

func New() *Services {
	return &Services{
		AuthService: auth.New(),
	}
}
