package config

type ServerConfig struct {
	Host string
	Port int
}

func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Host: "localhost",
		Port: 8080,
	}
}
