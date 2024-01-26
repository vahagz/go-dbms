package auth

type AuthServiceT struct {
}

type AuthService interface {
	ValidateCredentials(username string, password string) bool
}

func New() *AuthServiceT {
	return &AuthServiceT{}
}

func (as *AuthServiceT) ValidateCredentials(username string, password string) bool {
	return (username == "username" && password == "password")
}
