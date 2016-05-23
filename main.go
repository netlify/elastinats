package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"
	elastic "gopkg.in/olivere/elastic.v3"

	nconfig "github.com/netlify/util/config"
	nlog "github.com/netlify/util/logger"
	nnats "github.com/netlify/util/messaging"
)

var rootLog *logrus.Entry

type elasticConfig struct {
	Index    string   `json:"index"`
	Hosts    []string `json:"hosts"`
	UseHTTPS bool     `json:"use_https"`
}

type subjectAndGroup struct {
	Subject string `json:"subject"`
	Group   string `json:"group"`
}

type configuration struct {
	NatsConf    nnats.NatsConfig      `json:"nats_conf"`
	ElasticConf elasticConfig         `json:"elastic_conf"`
	LogConf     nlog.LogConfiguration `json:"log_conf"`
	Subjects    []subjectAndGroup     `json:"subjects"`
}

func main() {
	var cfgFile string
	rootCmd := cobra.Command{
		Short: "elastinat",
		Long:  "elastinat",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cfgFile)
		},
	}

	rootCmd.Flags().StringVarP(&cfgFile, "config", "c", "config.json", "the json config file")

	if err := rootCmd.Execute(); err != nil {
		if rootLog != nil {
			rootLog.WithError(err).Warn("Failed to execute command")
		}
		os.Exit(1)
	}
}

func run(configFile string) error {
	config := new(configuration)
	err := nconfig.LoadFromFile(configFile, config)
	if err != nil {
		return err
	}

	rootLog, err = nlog.ConfigureLogging(&config.LogConf)
	if err != nil {
		return err
	}

	scheme := "http"
	if config.ElasticConf.UseHTTPS {
		scheme = "https"
	}

	rootLog.WithFields(logrus.Fields{
		"hosts":     config.ElasticConf.Hosts,
		"use_https": config.ElasticConf.UseHTTPS,
		"index":     config.ElasticConf.Index,
	}).Info("Connecting to elastic search")

	client, err := elastic.NewClient(
		elastic.SetScheme(scheme),
		elastic.SetURL(config.ElasticConf.Hosts...),
	)
	if err != nil {
		return err
	}

	rootLog.WithFields(config.NatsConf.LogFields()).Info("Connecting to Nats")
	nc, err := nnats.ConnectToNats(&config.NatsConf)
	if err != nil {
		return err
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
			return err
		}

		wg.Add(1)
		f := func() {
			log.Info("Starting to consume")
			consumeForever(config.ElasticConf.Index, client, c, log)
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

	return nil
}

func consumeForever(index string, client *elastic.Client, natsSubj chan *nats.Msg, log *logrus.Entry) {
	for msg := range natsSubj {
		payload := make(map[string]interface{})

		// maybe it is json!
		_ = json.Unmarshal(msg.Data, &payload)

		payload["@raw_msg"] = string(msg.Data)
		payload["@timestamp"] = time.Now().Unix()
		payload["@source"] = msg.Subject
		_, err := client.Index().Index(index).Type("log_line").BodyJson(payload).Do()
		if err != nil {
			log.WithError(err).Warn("Error sending data to elasticsearch")
		}
	}
}
