package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
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

var pool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

func newPayload(msg, source string) *payload {
	return &payload{
		rawMsgKey:    msg,
		sourceKey:    source,
		timestampKey: time.Now().Format(time.RFC3339),
	}
}

type elasticConfig struct {
	Index           string   `json:"index"`
	Hosts           []string `json:"hosts"`
	Port            int      `json:"port"`
	BatchSize       int      `json:"batch_size"`
	BatchTimeoutSec int      `json:"batch_timeout_sec"`

	client *http.Client
}

func batchAndSend(config *elasticConfig, incoming <-chan payload, stats *counters, log *logrus.Entry) {
	if config.client == nil {
		config.client = &http.Client{
			Timeout: time.Second * 2,
		}
	}

	log = log.WithFields(logrus.Fields{
		"index": config.Index,
	})

	log.WithFields(logrus.Fields{
		"hosts":         config.Hosts,
		"port":          config.Port,
		"batch_size":    config.BatchSize,
		"batch_timeout": config.BatchTimeoutSec,
	}).Info("Starting to consume forever and batch send to ES")

	batch := make([]payload, 0, config.BatchSize)

	sendTimeout := time.Tick(time.Duration(config.BatchTimeoutSec) * time.Second)

	for {
		select {
		case in := <-incoming:
			batch = append(batch, in)
			if len(batch) >= config.BatchSize {
				log.WithField("size", len(batch)).Debug("Sending batch because of size")

				toSend := make([]payload, len(batch))
				copy(toSend, batch)
				batch = make([]payload, 0, config.BatchSize)

				go sendToES(config, log, stats, toSend)
			}
		case <-sendTimeout:
			log.WithField("size", len(batch)).Debug("Sending batch because of timeout")

			toSend := make([]payload, len(batch))
			copy(toSend, batch)
			batch = make([]payload, 0, config.BatchSize)

			go sendToES(config, log, stats, toSend)
		}
	}
}

func sendToES(config *elasticConfig, log *logrus.Entry, stats *counters, batch []payload) {
	if len(batch) == 0 {
		return
	}
	if config.client == nil {
		log.Warn("Tried to send with a nil client")
		return
	}

	log = log.WithFields(logrus.Fields{
		"size":     len(batch),
		"batch_id": rand.Int(),
	})

	// build the payload
	buff := pool.Get().(*bytes.Buffer)
	buff.Reset()
	defer pool.Put(buff)

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

	start := time.Now()
	resp, err := config.client.Post(endpoint, "text/plain", buff)
	elapsed := time.Since(start)
	if err != nil {
		log.WithError(err).WithField("endpoint", endpoint).Warn("Failed to post to elasticsearch")
	} else {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.WithError(err).Warn("Failed to read the response body")
		} else {
			if resp.StatusCode != 200 {
				log.Warn("Failed to post batch: %s", string(body))
			} else {
				log.WithField("elapsed", elapsed).Debugf("Completed post in %s: %s", elapsed, string(body))
				atomic.AddInt64(&(*stats).esSent, int64(len(batch)))
				atomic.AddInt64(&(*stats).batchesSent, 1)
			}
		}
	}
}
