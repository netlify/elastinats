package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/netlify/elastinats/conf"
	"github.com/netlify/elastinats/messaging"
	"github.com/netlify/elastinats/stats"
)

const (
	indexCommand = `{ "index": {} }`
)

var pool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

var client = http.Client{
	Timeout: time.Second * 2,
}

func BatchAndSend(config *conf.ElasticConfig, incoming <-chan messaging.Payload, stats *stats.Counters, log *logrus.Entry) chan<- bool {
	log.WithFields(logrus.Fields{
		"hosts":         config.Hosts,
		"port":          config.Port,
		"batch_size":    config.BatchSize,
		"batch_timeout": config.BatchTimeoutSec,
		"type":          config.Type,
	}).Info("Starting to consume forever and batch send to ES")

	batch := make([]messaging.Payload, 0, config.BatchSize)

	sendTimeout := time.Tick(time.Duration(config.BatchTimeoutSec) * time.Second)
	shutdown := make(chan bool)

	// spawn this off to a child routine
	go func() {
		for {
			select {
			case in := <-incoming:
				batch = append(batch, in)
				if len(batch) >= config.BatchSize {
					log.WithField("size", len(batch)).Debug("Sending batch because of size")

					toSend := make([]messaging.Payload, len(batch))
					copy(toSend, batch)
					batch = make([]messaging.Payload, 0, config.BatchSize)

					go sendToES(config, log, stats, toSend)
				}
			case <-sendTimeout:
				log.WithField("size", len(batch)).Debug("Sending batch because of timeout")

				toSend := make([]messaging.Payload, len(batch))
				copy(toSend, batch)
				batch = make([]messaging.Payload, 0, config.BatchSize)

				go sendToES(config, log, stats, toSend)
			case <-shutdown:
				log.Debug("Shutting down")
				break
			}
		}
	}()

	return shutdown
}

func sendToES(config *conf.ElasticConfig, log *logrus.Entry, stats *stats.Counters, batch []messaging.Payload) {
	if len(batch) == 0 {
		return
	}

	log = log.WithFields(logrus.Fields{
		"size":     len(batch),
		"batch_id": rand.Int(),
	})

	host := config.Hosts[rand.Intn(len(config.Hosts))]
	log = log.WithField("host", host)

	index, err := config.GetIndex(time.Now().UTC())
	if err != nil {
		log.Errorf("Failed to parse index from string %s", config.Index)
		return
	}
	log = log.WithField("index", index)

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

	// http://<HOST>:<PORT>/_index/_type -- encode the index and type here so we don't
	// send it in the body with each batch
	endpoint := fmt.Sprintf("http://%s:%d/%s/%s/_bulk", host, config.Port, index, config.Type)
	stats.IncrementBatchesSent()
	stats.IncrementMessagesSent(int64(len(batch)))

	start := time.Now()
	resp, err := client.Post(endpoint, "text/plain", buff)
	elapsed := time.Since(start)
	if err != nil {
		log.WithError(err).WithField("endpoint", endpoint).Warn("Failed to post to elasticsearch")
		stats.IncrementBatchesFailed()
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Warn("Failed to read the response body")
		return
	}

	if resp.StatusCode != 200 {
		log.Warnf("Failed to post batch: %s", string(body))
		stats.IncrementBatchesFailed()
		return
	}

	completeLog := log.WithFields(logrus.Fields{
		"index":       index,
		"host":        host,
		"endpoint":    endpoint,
		"status_code": resp.StatusCode,
	})

	if len(body) != 0 {
		// responds with json always - let's check for errors in it
		type response struct {
			Errors bool `json:"errors"`
			Items  []struct {
				Index struct {
					Error string `json:"error"`
				} `json:"index"`
			} `json:"items"`
		}
		parsed := new(response)
		err = json.Unmarshal(body, parsed)
		if err != nil {
			completeLog.WithError(err).Warnf("Failed to parse the response body: %s", string(body))
			return
		}

		if parsed.Errors {
			// we had some errors - lets collect them and let people know
			stats.IncrementBatchesFailed()

			errs := make(map[string]int)
			for _, item := range parsed.Items {
				errs[item.Index.Error] = errs[item.Index.Error] + 1
			}

			// make the empty error more obvious
			errs["no error"] = errs[""]
			delete(errs, "")

			type errReport struct {
				Msg   string `json:"msg"`
				Count int    `json:"count"`
			}
			report := []errReport{}
			for e, c := range errs {
				report = append(report, errReport{
					Msg:   e,
					Count: c,
				})
			}

			bs, err := json.Marshal(&report)
			if err != nil {
				completeLog.WithError(err).Warn("Failed to marshal error report")
				return
			}

			completeLog.Warn(string(bs))
		}
	}

	completeLog.WithFields(logrus.Fields{
		"elapsed": elapsed,
	}).Debugf("Completed post in %s", elapsed)
}
