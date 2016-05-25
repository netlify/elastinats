package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"github.com/netlify/messaging"
)

var rootLog *logrus.Entry

func main() {
	var cfgFile string
	rootCmd := cobra.Command{
		Short: "elastinat",
		Long:  "elastinat",
		Run: func(cmd *cobra.Command, args []string) {
			run(cfgFile)
		},
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "config.json", "the json config file")

	if err := rootCmd.Execute(); err != nil {
		if rootLog != nil {
			rootLog.WithError(err).Warn("Failed to execute command")
		}
		os.Exit(1)
	}
}

func run(configFile string) {
	config := new(configuration)
	err := loadFromFile(configFile, config)
	if err != nil {
		log.Fatalf("Failed to load configuation: %s %v", configFile, err)
	}

	rootLog, err = configureLogging(&config.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging")
	}

	conn, err := connectToES(config.ElasticConf)
	if err != nil {
		rootLog.WithError(err).Fatal("Failed to connect to elasticsearch")
	}
	rootLog.Info("Connected to elasticseach")

	rootLog.WithFields(config.NatsConf.LogFields()).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf)
	if err != nil {
		rootLog.WithError(err).Fatal("Failed to connect to nats")
	}

	wg := sync.WaitGroup{}
	funcs := make([]func(), 0, len(config.Subjects))

	for _, pair := range config.Subjects {
		log := rootLog.WithFields(logrus.Fields{
			"subject": pair.Subject,
			"group":   pair.Group,
			"index":   config.ElasticConf.Index,
		})
		log.Debug("Connecting channel")

		c := make(chan *nats.Msg)
		var err error
		if pair.Group == "" {
			_, err = nc.ChanSubscribe(pair.Subject, c)
		} else {
			_, err = nc.ChanQueueSubscribe(pair.Subject, pair.Group, c)
		}
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe")
		}

		wg.Add(1)
		f := func() {
			log.Info("Starting to consume")
			consumeForever(config.ElasticConf.Index, conn, c, log)
			log.Info("Finished consuming")
			wg.Done()
		}

		funcs = append(funcs, f)
	}

	for _, f := range funcs {
		go f()
	}

	wg.Wait()
	rootLog.Info("Shutting down")
}

func connectToES(config elasticConfig) (*elastigo.Conn, error) {
	rootLog.WithFields(logrus.Fields{
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
			rootLog.WithFields(logrus.Fields{
				"component": "es",
				"method":    method,
				"url":       url,
			}).Info(body)
		}
	}

	conn.Hosts = config.Hosts

	h, err := conn.Health()
	if err != nil {
		return nil, err
	}

	if h.Status != "green" {
		return nil, fmt.Errorf("The status of the cluster is not green it is %s", h.Status)
	}

	return conn, nil
}

func consumeForever(index string, client *elastigo.Conn, natsSubj chan *nats.Msg, log *logrus.Entry) {
	for msg := range natsSubj {
		payload := make(map[string]interface{})

		// maybe it is json!
		_ = json.Unmarshal(msg.Data, &payload)

		payload["@raw_msg"] = string(msg.Data)
		payload["@timestamp"] = time.Now().Unix()
		payload["@source"] = msg.Subject
		resp, err := client.Index(index, "log_line", "", nil, payload)
		if err != nil {
			log.WithError(err).Warn("Error sending data to elasticsearch")
		}

		log.Debugf("inserted line: %s", resp.Id)
	}
}
