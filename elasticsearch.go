package main

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mattbaird/elastigo/lib"
)

func connectToES(config elasticConfig, log *logrus.Entry) (*elastigo.Conn, error) {
	log.WithFields(logrus.Fields{
		"hosts": config.Hosts,
		"index": config.Index,
		"port":  config.Port,
		"trace": config.Trace,
	}).Info("Connecting to elastic search")

	conn := elastigo.NewConn()
	if config.Port > 0 {
		conn.SetPort(fmt.Sprintf("%d", config.Port))
	}

	if config.Trace {
		conn.RequestTracer = func(method, url, body string) {
			log.WithFields(logrus.Fields{
				"component": "es",
				"method":    method,
				"url":       url,
			}).Info(body)
		}
	}

	conn.Hosts = config.Hosts

	h, err := conn.Health(config.Index)
	if err != nil {
		return nil, err
	}

	if h.Status != "green" {
		log.Warnf("The cluster is not in good shape: %s", h.Status)
	}

	return conn, nil
}

func sendToES(config *elasticConfig, log *logrus.Entry, toSend <-chan *map[string]interface{}, stats *counters) {
	log = log.WithFields(logrus.Fields{
		"index": config.Index,
		"hosts": config.Hosts,
	})

	client, err := connectToES(*config, log)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to elasticsearch")
	}
	log.Info("Connected to elasticseach")

	for in := range toSend {
		payload := *in
		log.Debugf("raw: %s", payload["@raw_msg"])
		resp, err := client.Index(config.Index, "log_line", "", nil, payload)
		if err != nil {
			log.WithError(err).Warn("Error sending data to elasticsearch -- reconnecting")

			times := config.ReconnectAttempts
			for ; times > 0; times-- {
				client, err = connectToES(*config, log)
				if err != nil {
					log.WithError(err).Warn("Error sending data to elasticsearch -- reconnecting")
				} else {
					log.Info("Reconnected")
					break
				}
				time.Sleep(3 * time.Second)
			}

			if times == 0 {
				log.Fatalf("Failed to reconnect to ES after %d attempts", config.ReconnectAttempts)
			}
		} else {
			stats.esSent++
			log.WithField("id", resp.Id).Debug("send completed.")
		}
	}
}
