package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"text/template"
	"time"

	"github.com/netlify/messaging"
)

type configuration struct {
	NatsConf    messaging.NatsConfig `json:"nats_conf"`
	ElasticConf elasticConfig        `json:"elastic_conf"`
	LogConf     logConfiguration     `json:"log_conf"`
	Subjects    []subjectAndGroup    `json:"subjects"`
	ReportSec   int64                `json:"report_sec"`
}

type subjectAndGroup struct {
	Subject string `json:"subject"`
	Group   string `json:"group"`
}

type elasticConfig struct {
	Index           string   `json:"index"`
	Hosts           []string `json:"hosts"`
	Port            int      `json:"port"`
	BatchSize       int      `json:"batch_size"`
	BatchTimeoutSec int      `json:"batch_timeout_sec"`

	indexTemplate *template.Template
}

func (e *elasticConfig) GetIndex(t time.Time) (string, error) {
	if e.Index == "" {
		return "", errors.New("No index configured")
	}

	if e.indexTemplate == nil {
		var err error
		e.indexTemplate, err = template.New("index_template").Parse(e.Index)
		if err != nil {
			return "", err
		}
	}

	b := bytes.NewBufferString("")
	err := e.indexTemplate.Execute(b, t)

	return b.String(), err
}

func loadFromFile(configFile string) (*configuration, error) {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	config := new(configuration)
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
