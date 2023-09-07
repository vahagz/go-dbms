package logger

import (
	"os"

	logger "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var L = &logger.Logger{
	Out:   os.Stderr,
	Level: logger.DebugLevel,
	Formatter: &prefixed.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp: true,
		ForceFormatting: true,
	},
}
