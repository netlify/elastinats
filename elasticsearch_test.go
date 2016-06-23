package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var testLog = logrus.StandardLogger().WithField("testing", true)
var goodResponse = &http.Response{
	Body:       ioutil.NopCloser(bytes.NewBuffer(nil)),
	StatusCode: 200,
}
var loads = []payload{
	payload{"something": "borrowed"},
	payload{"something": "blue"},
	payload{"something": "old"},
	payload{"something": "new"},
}

func TestTimeoutSend(t *testing.T) {
	config := getConfig()
	config.BatchTimeoutSec = 1

	stats := sendAndSuch(t, config, []payload{
		payload{"something": "borrowed"},
		payload{"something": "blue"}})

	assert.Equal(t, int64(1), stats.batchesSent)
	assert.Equal(t, int64(0), stats.batchesFailed)
	assert.Equal(t, int64(2), stats.esSent)
}

func TestSendOnBatchSize(t *testing.T) {
	config := getConfig()
	config.BatchSize = 3

	stats := sendAndSuch(t, config, loads)

	// validate it was called
	assert.Equal(t, int64(1), stats.batchesSent)
	assert.Equal(t, int64(0), stats.batchesFailed)
	assert.Equal(t, int64(3), stats.esSent)
}

func TestErrorParsing(t *testing.T) {
	var req *http.Request
	config := getConfig()
	config.client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			response := `{ "errors": true }`
			badResponse := &http.Response{
				Body: ioutil.NopCloser(bytes.NewBufferString(response)),
				// sometimes ES returns a 200, but with error strings
				StatusCode: 200,
				Header:     http.Header{},
			}
			badResponse.Header.Set("Content-Type", "application/json")
			return badResponse, nil
		},
	}

	stats := new(counters)
	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	assert.Equal(t, int64(1), stats.batchesFailed)
	assert.Equal(t, int64(1), stats.batchesSent)
	assert.Equal(t, int64(4), stats.esSent)
}

func TestSendBatchMessageIsOk(t *testing.T) {
	var req *http.Request
	config := getConfig()
	config.client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			return goodResponse, nil
		},
	}

	stats := new(counters)

	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	assert.Equal(t, "/quotes/log_line/_bulk", req.URL.Path)
	validatePayload(t, req.Body, loads)
}

func TestErrorStatus(t *testing.T) {
	var req *http.Request
	config := getConfig()
	config.client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			badResponse := &http.Response{
				Body:       ioutil.NopCloser(bytes.NewBuffer(nil)),
				StatusCode: 500,
			}
			return badResponse, nil
		},
	}

	stats := new(counters)

	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	// 1 sent and failed
	assert.Equal(t, int64(1), stats.batchesFailed)
	assert.Equal(t, int64(1), stats.batchesSent)
	assert.Equal(t, int64(4), stats.esSent)
}

func TestMissingClient(t *testing.T) {
	config := getConfig()
	config.client = nil

	stats := new(counters)
	sendToES(config, testLog, stats, []payload{})

	assert.Equal(t, int64(0), stats.batchesSent)
	assert.Equal(t, int64(0), stats.batchesFailed)
	assert.Equal(t, int64(0), stats.esSent)
}

func TestSkipEmpties(t *testing.T) {
	config := getConfig()
	config.client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			assert.FailNow(t, "Shouldn't have sent anything")
			return nil, nil
		},
	}
	stats := new(counters)
	sendToES(config, testLog, stats, []payload{})

	assert.Equal(t, int64(0), stats.batchesSent)
	assert.Equal(t, int64(0), stats.batchesFailed)
	assert.Equal(t, int64(0), stats.esSent)
}

// --------------------------------------------------------------------------------------------------------------------

func getConfig() *elasticConfig {
	return &elasticConfig{
		Index:           "quotes",
		Hosts:           []string{"first", "second"},
		Port:            80,
		BatchSize:       10,
		BatchTimeoutSec: 10,
		client:          &http.Client{},
	}
}

type testTransport struct {
	delegate func(*http.Request) (*http.Response, error)
}

func (tt testTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return tt.delegate(r)
}

func sendAndSuch(t *testing.T, config *elasticConfig, payloads []payload) *counters {
	reqChan := make(chan *http.Request)
	config.client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			reqChan <- r
			return goodResponse, nil
		},
	}

	in := make(chan payload)
	stats := new(counters)

	shutdown := batchAndSend(config, in, stats, testLog)
	defer func() {
		shutdown <- true
	}()

	for _, p := range payloads {
		in <- p
	}

	// have to do it this way b/c otherwise it is a race
	select {
	case req := <-reqChan:
		assert.NotNil(t, req)
		assert.Equal(t, "/quotes/log_line/_bulk", req.URL.Path)
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "timed out waiting for request")
	}

	return stats
}

func validatePayload(t *testing.T, body io.ReadCloser, payloads []payload) {
	assert.NotNil(t, body)

	defer body.Close()
	raw, err := ioutil.ReadAll(body)
	assert.Nil(t, err)
	assert.NotNil(t, raw)

	str := string(raw)
	assert.True(t, strings.HasSuffix(str, "\n"))

	for i, entry := range strings.Split(str, "\n") {
		if entry != "" {
			if i%2 == 0 {
				assert.JSONEq(t, `{ "index": {} }`, entry)
			} else {
				asStr, err := json.Marshal(payloads[i/2])
				assert.Nil(t, err)
				assert.JSONEq(t, string(asStr), entry)
			}
		}
	}
}
