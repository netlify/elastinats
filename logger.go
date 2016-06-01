package main

import (
	"bufio"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
)

type logConfiguration struct {
	Level string `json:"log_level"`
	File  string `json:"log_file"`
}

func configureLogging(cfg *logConfiguration) (*logrus.Entry, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if cfg.File != "" {
		f, errOpen := os.OpenFile(cfg.File, os.O_RDWR|os.O_APPEND, 0660)
		if errOpen != nil {
			return nil, errOpen
		}
		logrus.SetOutput(bufio.NewWriter(f))
	}

	level, err := logrus.ParseLevel(strings.ToUpper(cfg.Level))
	if err != nil {
		return nil, err
	}
	logrus.SetLevel(level)

	return logrus.StandardLogger().WithField("hostname", hostname), nil
}
