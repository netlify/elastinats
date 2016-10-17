package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/netlify/elastinats/conf"
	"github.com/netlify/elastinats/messaging"
	"github.com/netlify/elastinats/stats"
	"github.com/stretchr/testify/assert"
)

var testLog = logrus.StandardLogger().WithField("testing", true)
var goodResponse = &http.Response{
	Body:       ioutil.NopCloser(bytes.NewBufferString(`{"errors": false}`)),
	StatusCode: 200,
}
var loads = []messaging.Payload{
	{"something": "borrowed"},
	{"something": "blue"},
	{"something": "old"},
	{"something": "new"},
}

func TestTimeoutSend(t *testing.T) {
	config := getConfig()
	config.BatchTimeoutSec = 1

	stats := sendAndSuch(t, config, []messaging.Payload{
		{"something": "borrowed"},
		{"something": "blue"}})

	validateStats(t, stats, 1, 2, 0)
}

func TestSendOnBatchSize(t *testing.T) {
	config := getConfig()
	config.BatchSize = 3

	stats := sendAndSuch(t, config, loads)

	// validate it was called
	validateStats(t, stats, 1, 3, 0)
}

func TestErrorParsing(t *testing.T) {
	var req *http.Request
	config := getConfig()
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			response := `{"errors": true, "items":[{"index": {"error": "this is an error"}}]}`
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

	stats := new(stats.Counters)
	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	validateStats(t, stats, 1, 4, 1)
}

func TestSendBatchMessageIsOk(t *testing.T) {
	var req *http.Request
	config := getConfig()
	stats := stats.NewCounter(config)
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			return goodResponse, nil
		},
	}

	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	assert.Equal(t, "/quotes/log_line/_bulk", req.URL.Path)
	validateStats(t, stats, 1, 4, 0)
	validatePayload(t, req.Body, loads)
}

func TestErrorStatus(t *testing.T) {
	var req *http.Request
	config := getConfig()
	stats := stats.NewCounter(config)
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			req = r
			badResponse := &http.Response{
				Body:       ioutil.NopCloser(bytes.NewBufferString("something")),
				StatusCode: 500,
			}
			return badResponse, nil
		},
	}

	sendToES(config, testLog, stats, loads)

	assert.NotNil(t, req)
	validateStats(t, stats, 1, 4, 1)
}

func TestMissingClient(t *testing.T) {
	config := getConfig()
	stats := new(stats.Counters)
	sendToES(config, testLog, stats, []messaging.Payload{})

	validateStats(t, stats, 0, 0, 0)
}

func TestSkipEmpties(t *testing.T) {
	config := getConfig()
	stats := new(stats.Counters)

	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			assert.FailNow(t, "Shouldn't have sent anything")
			return nil, nil
		},
	}
	sendToES(config, testLog, stats, []messaging.Payload{})

	validateStats(t, stats, 0, 0, 0)
}

func TestIndexFormatIsRespected(t *testing.T) {
	config := getConfig()
	stats := new(stats.Counters)

	config.Index = "test_{{.Year}}_{{.Month}}_{{.Day}}"
	now := time.Now().UTC()
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			dateString := strings.ToLower(fmt.Sprintf("test_%d_%s_%d", now.Year(), now.Month(), now.Day()))
			assert.Equal(t, "/"+dateString+"/log_line/_bulk", r.URL.Path)
			return goodResponse, nil
		},
	}

	sendToES(config, testLog, stats, loads)
}

func TestBadFormatForIndex(t *testing.T) {
	config := getConfig()
	config.Index = "test_{{if }}"
	stats := new(stats.Counters)
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			assert.FailNow(t, "shouldn't have sent anything")
			return nil, nil
		},
	}

	sendToES(config, testLog, stats, loads)
}

// --------------------------------------------------------------------------------------------------------------------

func getConfig() *conf.ElasticConfig {
	c := conf.ElasticConfig{
		Index:           "quotes",
		Hosts:           []string{"first", "second"},
		Type:            "log_line",
		Port:            80,
		BatchSize:       10,
		BatchTimeoutSec: 10,
	}
	return &c
}

type testTransport struct {
	delegate func(*http.Request) (*http.Response, error)
}

func (tt testTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return tt.delegate(r)
}

func sendAndSuch(t *testing.T, config *conf.ElasticConfig, payloads []messaging.Payload) *stats.Counters {
	reqChan := make(chan *http.Request)
	client.Transport = testTransport{
		delegate: func(r *http.Request) (*http.Response, error) {
			reqChan <- r
			return goodResponse, nil
		},
	}

	in := make(chan messaging.Payload)
	stats := new(stats.Counters)

	shutdown := BatchAndSend(config, in, stats, testLog)
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
		assert.Equal(t, fmt.Sprintf("/quotes/%s/_bulk", config.Type), req.URL.Path)
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "timed out waiting for request")
	}

	return stats
}

func validateStats(t *testing.T, stats *stats.Counters, batchesSent, linesSent, batchesFailed int) {
	assert.EqualValues(t, batchesSent, stats.BatchesSent)
	assert.EqualValues(t, batchesFailed, stats.BatchesFailed)
	assert.EqualValues(t, linesSent, stats.MessagesSent)
}

func validatePayload(t *testing.T, body io.ReadCloser, payloads []messaging.Payload) {
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
