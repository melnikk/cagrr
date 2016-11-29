package cagrr

import (
	"github.com/Sirupsen/logrus"
	"github.com/fatih/structs"
	"github.com/rifflock/lfshook"
)

var (
	log Logger
)

// NewLogger cretes an implementation of Logger
func NewLogger(verb, filename string) Logger {
	level, _ := logrus.ParseLevel(verb)
	logrus.SetLevel(level)
	if filename != "stdout" && filename != "" {

		logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.AddHook(lfshook.NewHook(lfshook.PathMap{
			logrus.InfoLevel:  filename,
			logrus.DebugLevel: filename,
			logrus.WarnLevel:  filename,
			logrus.ErrorLevel: filename,
		}))
	}

	log = logger{}
	return log
}

func (l logger) WithFields(str interface{}) Logger {
	l.fields = structs.Map(str)
	return l
}
func (l logger) WithError(err error) Logger {
	l.err = err
	return l
}
func (l logger) Debug(message interface{}) Logger {
	logrus.WithFields(l.fields).Debug(message)
	return l
}
func (l logger) Error(message interface{}) Logger {
	logrus.WithError(l.err).WithFields(l.fields).Error(message)
	return l
}
func (l logger) Fatal(message interface{}) Logger {
	logrus.WithError(l.err).WithFields(l.fields).Fatal(message)
	return l
}
func (l logger) Warn(message interface{}) Logger {
	logrus.WithError(l.err).WithFields(l.fields).Warn(message)
	return l
}
func (l logger) Info(message interface{}) Logger {
	logrus.WithFields(l.fields).Info(message)
	return l
}
