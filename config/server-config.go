package config

type ServerConfig struct {
	Host        string
	Port        uint
	AuthTimeout uint
}

func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Host:        "localhost",
		Port:        8080,
		AuthTimeout: 10,
	}
}
