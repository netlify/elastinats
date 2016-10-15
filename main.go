package main

import (
	"log"

	"github.com/Sirupsen/logrus"

	"github.com/netlify/elastinats/cmd"
)

var rootLog *logrus.Entry

type counters struct {
	natsConsumed  int64
	esSent        int64
	batchesSent   int64
	batchesFailed int64
}

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		log.Fatal("error during command: " + err.Error())
	}
}
