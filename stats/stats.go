package stats

import (
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"

	"sync/atomic"

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

func (c *Counters) ReportStats(reportSec time.Duration, log *logrus.Entry) {
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
			"messages_rx":    c.MessagsConsumed,
			"messages_tx":    c.MessagesSent,
			"batches_tx":     c.BatchesSent,
			"batches_failed": c.BatchesFailed,
			"batch_size":     c.BatchSize,
			"batch_timeout":  c.BatchTimeout,
		}).Info("status report")
	}
}
