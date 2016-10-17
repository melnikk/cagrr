package logs

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/fatih/structs"
	"github.com/melnikk/logrus-rabbitmq-hook"
)

// Logger logs messages
type Logger interface {
	WithError(err error) Logger
	WithFields(str interface{}) Logger
	Debug(message interface{}) Logger
	Error(message interface{}) Logger
	Fatal(message interface{}) Logger
	Warn(message interface{}) Logger
	Info(message interface{}) Logger
}

type logger struct {
	err    error
	fields map[string]interface{}
}

// CreateLogger cretes an implementation of Logger
func CreateLogger(verb, index, app string) Logger {
	level, _ := logrus.ParseLevel(verb)
	logrus.SetLevel(level)

	url := os.Getenv("LOG_STREAM_URL")
	if url != "" {
		hook := hook.New(index, url, app, app)
		logrus.AddHook(hook)
	}

	log := logger{}
	result := Logger(log)
	return result
}

func (l logger) WithFields(str interface{}) Logger {
	l.fields = structs.Map(str)
	return Logger(l)
}
func (l logger) WithError(err error) Logger {
	l.err = err
	return Logger(l)
}
func (l logger) Debug(message interface{}) Logger {
	logrus.WithFields(l.fields).Debug(message)
	return Logger(l)
}
func (l logger) Error(message interface{}) Logger {
	logrus.WithError(l.err).WithFields(l.fields).Error(message)
	return Logger(l)
}
func (l logger) Fatal(message interface{}) Logger {
	logrus.WithFields(l.fields).Fatal(message)
	return Logger(l)
}
func (l logger) Warn(message interface{}) Logger {
	logrus.WithFields(l.fields).Warn(message)
	return Logger(l)
}
func (l logger) Info(message interface{}) Logger {
	logrus.WithFields(l.fields).Info(message)
	return Logger(l)
}
