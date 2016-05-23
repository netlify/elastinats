package main

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"

	"github.com/netlify/messaging"
)

type configuration struct {
	NatsConf    messaging.NatsConfig `json:"nats_conf"`
	ElasticConf elasticConfig        `json:"elastic_conf"`
	LogConf     logConfiguration     `json:"log_conf"`
	Subjects    []subjectAndGroup    `json:"subjects"`
}

type elasticConfig struct {
	Index    string   `json:"index"`
	Hosts    []string `json:"hosts"`
	UseHTTPS bool     `json:"use_https"`
}

type subjectAndGroup struct {
	Subject string `json:"subject"`
	Group   string `json:"group"`
}

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

func loadFromFile(configFile string, configStruct interface{}) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, configStruct)
	if err != nil {
		return err
	}

	return nil
}
