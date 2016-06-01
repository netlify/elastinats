package main

import (
	"encoding/json"
	"io/ioutil"

	"github.com/netlify/messaging"
)

type configuration struct {
	NatsConf    messaging.NatsConfig `json:"nats_conf"`
	ElasticConf elasticConfig        `json:"elastic_conf"`
	LogConf     logConfiguration     `json:"log_conf"`
	Subjects    []subjectAndGroup    `json:"subjects"`
	ReportSec   int64                `json:"report_sec"`
}

type elasticConfig struct {
	Index             string   `json:"index"`
	Hosts             []string `json:"hosts"`
	Port              int      `json:"port"`
	Trace             bool     `json:"trace"`
	ReconnectAttempts int      `json:"reconnect_attempts"`
}

type subjectAndGroup struct {
	Subject string `json:"subject"`
	Group   string `json:"group"`
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
