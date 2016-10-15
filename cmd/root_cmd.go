package cmd

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/netlify/messaging"
	"github.com/spf13/cobra"

	"github.com/netlify/elastinats/conf"
	"github.com/netlify/elastinats/elastic"
	"github.com/netlify/elastinats/message"
	"github.com/netlify/elastinats/stats"
)

var rootCmd = &cobra.Command{
	Short: "elastinat",
	Long:  "elastinat",
	Run:   run,
}

func RootCmd() *cobra.Command {
	rootCmd.PersistentFlags().StringP("config", "c", "", "a config file to use")
	rootCmd.AddCommand(versionCmd)

	return rootCmd
}

func run(cmd *cobra.Command, _ []string) {
	config, err := conf.LoadConfig(cmd)
	if err != nil {
		log.Fatalf("Failed to load configuation: %v", err)
	}

	rootLogger, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		log.Fatal("Failed to configure logging")
	}

	rootLogger.Info("Configured - starting to connect and consume")

	// connect to NATS
	rootLogger.WithFields(config.NatsConf.LogFields()).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf)
	if err != nil {
		rootLogger.WithError(err).Fatal("Failed to connect to nats")
	}

	var defaultConsumer nats.MsgHandler
	if config.ElasticConf != nil {
		defaultConsumer = buildConsumer(config.ElasticConf, config.ReportSec, rootLogger)
	}

	for _, pair := range config.Subjects {
		log := rootLogger.WithFields(logrus.Fields{
			"subject": pair.Subject,
			"group":   pair.Group,
		})
		log.Debug("Connecting channel")

		// connect ~ does it go to the default or a custom one?
		cons := defaultConsumer
		if pair.Endpoint != nil {
			log.Debugf("Starting consumer for endpoint %+v", pair.Endpoint)
			cons = buildConsumer(pair.Endpoint, config.ReportSec, log)
		}

		_ = cons
		var err error
		if pair.Group == "" {
			_, err = nc.Subscribe(pair.Subject, cons)
		} else {
			_, err = nc.QueueSubscribe(pair.Subject, pair.Group, cons)
		}

		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe")
		}

		log.Info("Started consuming from subject")
	}

	rootLogger.Info("Subscribed to all subject/groups - waiting")
	select {}
}

func buildConsumer(el *conf.ElasticConfig, reportSec int64, log *logrus.Entry) nats.MsgHandler {
	// create a channel to use
	c := make(chan message.Payload)
	stats := stats.NewCounter(el)
	interval := time.Second * time.Duration(reportSec)
	go stats.ReportStats(interval, log.WithFields(logrus.Fields{
		"index": el.Index,
		"type":  el.Type,
	}))

	elastic.BatchAndSend(el, c, stats, log)

	return func(m *nats.Msg) {
		stats.IncrementMessagesConsumed()
		go func() {
			payload := message.NewPayload(string(m.Data), m.Subject)

			// maybe it is json!
			_ = json.Unmarshal(m.Data, payload)

			c <- *payload
		}()
	}
}
