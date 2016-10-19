package cmd

import (
	"encoding/json"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"github.com/netlify/elastinats/conf"
	"github.com/netlify/elastinats/elastic"
	"github.com/netlify/elastinats/messaging"
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
		logrus.Fatalf("Failed to load configuation: %v", err)
	}

	rootLogger, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		logrus.Fatal("Failed to configure logging")
	}

	rootLogger.WithField("version", Version).Info("Configured - starting to connect and consume")

	// connect to NATS
	rootLogger.WithFields(logrus.Fields{
		"servers":   config.NatsConf.Servers,
		"ca_files":  config.NatsConf.CAFiles,
		"key_file":  config.NatsConf.KeyFile,
		"cert_file": config.NatsConf.CertFile,
	}).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf, errorReporter(rootLogger))
	if err != nil {
		rootLogger.WithError(err).Fatal("Failed to connect to nats")
	}

	var defaultConsumer nats.MsgHandler
	var defaultStats *stats.Counters
	if config.ElasticConf != nil {
		rootLogger.Debug("Starting default Consumer")
		defaultStats, defaultConsumer = buildConsumer(config.ElasticConf, config.BufferSize, rootLogger)
	}

	for _, pair := range config.Subjects {
		log := rootLogger.WithFields(logrus.Fields{
			"subject": pair.Subject,
			"group":   pair.Group,
		})
		log.Debug("Connecting channel")

		// connect ~ does it go to the default or a custom one?
		cons := defaultConsumer
		st := defaultStats
		if pair.Endpoint == nil && defaultConsumer == nil {
			log.Fatal("No consumer provided and there is no default handler")
		} else if pair.Endpoint == nil {
			log.Debug("Using default consumer")
		} else {
			log.WithFields(logrus.Fields{
				"hosts": pair.Endpoint.Hosts,
				"type":  pair.Endpoint.Type,
				"index": pair.Endpoint.Index,
			}).Debugf("Starting consumer for endpoint")
			st, cons = buildConsumer(pair.Endpoint, config.BufferSize, log)
		}

		// subscribe ~ queue or alone
		var err error
		var sub *nats.Subscription
		if pair.Group == "" {
			log.Debug("Subscribing")
			sub, err = nc.Subscribe(pair.Subject, cons)
		} else {
			log.Debug("Subscribing to Queue")
			sub, err = nc.QueueSubscribe(pair.Subject, pair.Group, cons)
		}
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe")
		}

		if err = sub.SetPendingLimits(-1, -1); err != nil {
			log.WithError(err).Fatal("Failed to unlimit pending limits")
		}

		st.StartReporting(config.ReportSec, nc, sub, log)
		log.Info("Started consuming from subject")
	}

	rootLogger.Info("Subscribed to all subject/groups - waiting")
	select {}
}

func errorReporter(log *logrus.Entry) nats.ErrHandler {
	return func(_ *nats.Conn, sub *nats.Subscription, err error) {
		pendingMsgs, pendingBytes, _ := sub.Pending()
		droppedMsgs, _ := sub.Dropped()
		maxMsgs, maxBytes, _ := sub.PendingLimits()

		log.WithError(err).WithFields(logrus.Fields{
			"subject":           sub.Subject,
			"queue":             sub.Queue,
			"pending_msgs":      pendingMsgs,
			"pending_bytes":     pendingBytes,
			"max_msgs_pending":  maxMsgs,
			"max_bytes_pending": maxBytes,
			"dropped_msgs":      droppedMsgs,
		}).Warn("Error while consuming from nats")
	}
}

func buildConsumer(el *conf.ElasticConfig, bufferSize int64, log *logrus.Entry) (*stats.Counters, nats.MsgHandler) {
	stats := stats.NewCounter(el)

	c := make(chan messaging.Payload, bufferSize)
	elastic.BatchAndSend(el, c, stats, log)

	return stats, func(m *nats.Msg) {
		stats.IncrementMessagesConsumed()
		go func() {
			payload := messaging.NewPayload(string(m.Data), m.Subject)

			// maybe it is json!
			_ = json.Unmarshal(m.Data, payload)

			c <- *payload
		}()
	}
}
