package main

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"github.com/netlify/messaging"
)

var rootLog *logrus.Entry

type counters struct {
	natsConsumed  int64
	esSent        int64
	batchesSent   int64
	batchesFailed int64
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
	config, err := loadFromFile(configFile)
	if err != nil {
		log.Fatalf("Failed to load configuation: %s %v", configFile, err)
	}

	rootLog, err = configureLogging(&config.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging")
	}

	rootLog.Info("Configured - starting to connect and consume")

	clientChannel := make(chan payload)
	stats := new(counters)
	go reportStats(config.ReportSec, config, stats, rootLog)

	// non-blocking call, it launches a routine to do the consuming
	batchAndSend(&config.ElasticConf, clientChannel, stats, rootLog)

	// connect to NATS
	rootLog.WithFields(config.NatsConf.LogFields()).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf)
	if err != nil {
		rootLog.WithError(err).Fatal("Failed to connect to nats")
	}

	// build all the tailers
	for _, pair := range config.Subjects {
		log := rootLog.WithFields(logrus.Fields{
			"subject": pair.Subject,
			"group":   pair.Group,
		})
		log.Debug("Connecting channel")

		var err error
		if pair.Group == "" {
			_, err = nc.Subscribe(pair.Subject, processMsg(clientChannel, stats))
		} else {
			_, err = nc.QueueSubscribe(pair.Subject, pair.Group, processMsg(clientChannel, stats))
		}
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe")
		}
	}

	rootLog.Info("Subscribed to all subject/groups - waiting")
	select {}
}

func processMsg(clientChannel chan<- payload, stats *counters) func(*nats.Msg) {
	// DO NOT BLOCK
	// nats is truely a fire and forget, we need to get make sure we are ready to
	// take off the subject immediately. And we can have tons of go routines so
	// this seems like the natural pairing.
	return func(m *nats.Msg) {
		stats.natsConsumed++
		go func() {
			payload := newPayload(string(m.Data), m.Subject)

			// maybe it is json!
			_ = json.Unmarshal(m.Data, payload)

			clientChannel <- *payload
		}()
	}
}

func reportStats(reportSec int64, config *configuration, stats *counters, log *logrus.Entry) {
	if reportSec == 0 {
		log.Debug("Stats reporting disabled")
		return
	}

	ticks := time.Tick(time.Second * time.Duration(reportSec))
	memstats := new(runtime.MemStats)
	for range ticks {
		runtime.ReadMemStats(memstats)

		log.WithFields(logrus.Fields{
			"go_routines":    runtime.NumGoroutine(),
			"total_alloc":    memstats.TotalAlloc,
			"current_alloc":  memstats.Alloc,
			"mem_sys":        memstats.Sys,
			"mallocs":        memstats.Mallocs,
			"frees":          memstats.Frees,
			"heap_in_use":    memstats.HeapInuse,
			"heap_idle":      memstats.HeapIdle,
			"heap_sys":       memstats.HeapSys,
			"heap_released":  memstats.HeapReleased,
			"messages_rx":    stats.natsConsumed,
			"messages_tx":    stats.esSent,
			"batches_tx":     stats.batchesSent,
			"batches_failed": stats.batchesFailed,
			"batch_size":     config.ElasticConf.BatchSize,
			"batch_timeout":  config.ElasticConf.BatchTimeoutSec,
		}).Info("status report")
	}
}
