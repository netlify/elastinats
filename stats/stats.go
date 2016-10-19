package stats

import (
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"

	"sync/atomic"

	"github.com/nats-io/nats"
	"github.com/netlify/elastinats/conf"
)

type Counters struct {
	MessagsConsumed int64
	MessagesSent    int64
	BatchesSent     int64
	BatchesFailed   int64

	Index        string
	BatchSize    int
	BatchTimeout int
}

func NewCounter(el *conf.ElasticConfig) *Counters {
	return &Counters{
		BatchSize:    el.BatchSize,
		Index:        el.Index,
		BatchTimeout: el.BatchTimeoutSec,
	}
}

func (c *Counters) IncrementMessagesConsumed() int64 {
	return atomic.AddInt64(&c.MessagsConsumed, 1)
}

func (c *Counters) IncrementBatchesSent() {
	atomic.AddInt64(&c.BatchesSent, 1)
}

func (c *Counters) IncrementBatchesFailed() {
	atomic.AddInt64(&c.BatchesFailed, 1)
}

func (c *Counters) IncrementMessagesSent(val int64) {
	atomic.AddInt64(&c.MessagesSent, val)
}

func (c *Counters) StartReporting(reportSec int64, nc *nats.Conn, sub *nats.Subscription, log *logrus.Entry) {
	if reportSec == 0 {
		log.Debug("Stats reporting disabled")
		return
	}

	dur := time.Duration(reportSec) * time.Second

	go func() {
		ticks := time.Tick(dur)
		log.Debugf("Starting to report stats every %s", dur.String())
		for range ticks {
			reportStats(c, nc, sub, log)
		}
	}()
}

func reportStats(c *Counters, nc *nats.Conn, sub *nats.Subscription, log *logrus.Entry) {
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)

	pendingMsgs, pendingBytes, err := sub.Pending()
	if err != nil {
		log.WithError(err).Warn("Failed to get pending information")
	}

	deliveredMsgs, err := sub.Delivered()
	if err != nil {
		log.WithError(err).Warn("Failed to get delivered msgs")
	}

	droppedMsgs, err := sub.Dropped()
	if err != nil {
		log.WithError(err).Warn("Failed to get dropped msgs")
	}

	log.WithFields(logrus.Fields{
		"pending_msgs":   pendingMsgs,
		"pending_bytes":  pendingBytes,
		"delivered_msgs": deliveredMsgs,
		"dropped_msgs":   droppedMsgs,

		"go_routines":   runtime.NumGoroutine(),
		"total_alloc":   memstats.TotalAlloc,
		"current_alloc": memstats.Alloc,
		"mem_sys":       memstats.Sys,
		"mallocs":       memstats.Mallocs,
		"frees":         memstats.Frees,
		"heap_in_use":   memstats.HeapInuse,
		"heap_idle":     memstats.HeapIdle,
		"heap_sys":      memstats.HeapSys,
		"heap_released": memstats.HeapReleased,

		"messages_rx_nc": nc.InMsgs,
		"messages_tx_nc": nc.OutMsgs,
		"bytes_rx_nc":    nc.InBytes,
		"bytes_tx_nc":    nc.OutBytes,

		"messages_rx":    c.MessagsConsumed,
		"messages_tx":    c.MessagesSent,
		"batches_tx":     c.BatchesSent,
		"batches_failed": c.BatchesFailed,
		"batch_size":     c.BatchSize,
		"batch_timeout":  c.BatchTimeout,
	}).Info("status report")
}
