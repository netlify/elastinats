package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
)

const (
	rawMsgKey    = "@raw_msg"
	timestampKey = "@timestamp"
	sourceKey    = "@source"

	indexCommand = `{ "index": {} }`
)

type payload map[string]interface{}

func newPayload(msg, source string) *payload {
	return &payload{
		rawMsgKey:    msg,
		sourceKey:    source,
		timestampKey: time.Now().Format(time.RFC3339),
	}
}

type elasticConfig struct {
	Index             string   `json:"index"`
	Hosts             []string `json:"hosts"`
	Port              int      `json:"port"`
	Trace             bool     `json:"trace"`
	ReconnectAttempts int      `json:"reconnect_attempts"`
	BatchSize         int      `json:"batch_size"`
	BatchTimeoutSec   int      `json:"batch_timeout_sec"`
}

func batchAndSend(config *elasticConfig, incoming <-chan *payload, stats *counters, log *logrus.Entry) {
	log = log.WithFields(logrus.Fields{
		"index": config.Index,
	})

	log.WithFields(logrus.Fields{
		"hosts":         config.Hosts,
		"port":          config.Port,
		"trace":         config.Trace,
		"batch_size":    config.BatchSize,
		"batch_timeout": config.BatchTimeoutSec,
	}).Info("Starting to consume forever and batch send to ES")

	batch := make([]*payload, 0, config.BatchSize)

	for {
		select {
		case in := <-incoming:
			batch = append(batch, in)
			if len(batch) >= config.BatchSize {
				log.WithField("size", len(batch)).Debug("Sending batch because of size")
				go sendToES(config, log, stats, batch)
				batch = make([]*payload, 0, config.BatchSize)
			}
		case <-time.After(time.Duration(config.BatchTimeoutSec) * time.Second):
			log.WithField("size", len(batch)).Debug("Sending batch because of timeout")
			go sendToES(config, log, stats, batch)
			batch = make([]*payload, 0, config.BatchSize)
		}
	}
}

func sendToES(config *elasticConfig, log *logrus.Entry, stats *counters, batch []*payload) {
	if len(batch) == 0 {
		return
	}

	log = log.WithFields(logrus.Fields{
		"size":     len(batch),
		"batch_id": rand.Int(),
	})

	// build the payload
	buff := bytes.NewBuffer(nil)
	for _, in := range batch {
		// serialize the payload
		asBytes, err := json.Marshal(in)
		if err == nil {
			// we don't have to specify the _index || _type b/c we are going to
			// encode that in the URL. Hence the simple index command
			buff.WriteString(indexCommand)
			buff.WriteRune('\n')
			buff.Write(asBytes)
			buff.WriteRune('\n')
		} else {
			log.WithError(err).Warn("Failed to marshal the input")
		}
	}

	host := config.Hosts[rand.Intn(len(config.Hosts))]

	// http://<HOST>:<PORT>/_index/_type -- encode the index and type here so we don't
	// send it in the body with each batch
	endpoint := fmt.Sprintf("http://%s:%d/%s/%s/_bulk", host, config.Port, config.Index, "log_line")

	resp, err := http.Post(endpoint, "text/plain", buff)
	if err != nil {
		log.WithError(err).WithField("endpoint", endpoint).Warn("Failed to post to elasticsearch")
	} else {
		if resp.StatusCode != 200 {
			log.Warn("Failed to post batch")
		} else {
			log.Debug("Completed post")
			stats.esSent += int64(len(batch))
			stats.batchesSent++
		}
	}
}
