package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"github.com/netlify/messaging"
)

var rootLog *logrus.Entry

type counters struct {
	natsConsumed int64
	esSent       int64
}

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

	rootLog.Info("Configured - starting to connect and consume")

	// connect to ES
	clientChannel := make(chan *map[string]interface{})
	stats := new(counters)
	go reportStats(config.ReportSec, stats, rootLog)

	go sendToES(&config.ElasticConf, rootLog, clientChannel, stats)

	// connect to NATS
	rootLog.WithFields(config.NatsConf.LogFields()).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf)
	if err != nil {
		rootLog.WithError(err).Fatal("Failed to connect to nats")
	}

	// build all the tailers
	wg := sync.WaitGroup{}
	funcs := make([]func(), 0, len(config.Subjects))
	for _, pair := range config.Subjects {
		log := rootLog.WithFields(logrus.Fields{
			"subject": pair.Subject,
			"group":   pair.Group,
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
			consumeForever(c, clientChannel, stats)
			log.Info("Finished consuming")
			wg.Done()
		}

		funcs = append(funcs, f)
	}

	// launch all the tailers
	for _, f := range funcs {
		go f()
	}

	wg.Wait()
	rootLog.Info("Shutting down")
}

func consumeForever(natsSubj chan *nats.Msg, toSend chan<- *map[string]interface{}, stats *counters) {
	for msg := range natsSubj {
		stats.natsConsumed++
		m := msg

		// DO NOT BLOCK
		// nats is truely a fire and forget, we need to get make sure we are ready to
		// take off the subject immediately. And we can have tons of go routines so
		// this seems like the natural pairing.
		go func() {
			payload := make(map[string]interface{})

			// maybe it is json!
			_ = json.Unmarshal(m.Data, &payload)

			payload["@raw_msg"] = string(m.Data)
			payload["@timestamp"] = time.Now().Format(time.RFC3339)
			payload["@source"] = m.Subject

			toSend <- &payload
		}()
	}
}

func reportStats(reportSec int64, stats *counters, log *logrus.Entry) {
	if reportSec == 0 {
		return
	}

	ticks := time.Tick(time.Second * time.Duration(reportSec))
	for range ticks {
		log.WithFields(logrus.Fields{
			"messages_rx": stats.natsConsumed,
			"messages_tx": stats.esSent,
		}).Info("processed messages from nats to es")
	}
}
