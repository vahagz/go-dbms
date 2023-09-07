package config

type AppConfig struct {
	ServerConfig *ServerConfig
}

func New() *AppConfig {
	return &AppConfig{
		ServerConfig: NewServerConfig(),
	}
}
