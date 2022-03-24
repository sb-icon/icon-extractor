package main

import (
	"github.com/sudoblockio/icon-extractor/api"
	"github.com/sudoblockio/icon-extractor/config"
	"github.com/sudoblockio/icon-extractor/extractor"
	"github.com/sudoblockio/icon-extractor/global"
	"github.com/sudoblockio/icon-extractor/kafka"
	"github.com/sudoblockio/icon-extractor/logging"
	"github.com/sudoblockio/icon-extractor/transformer"
)

func main() {
	config.ReadEnvironment()

	logging.Init()

	kafka.StartProducers()
	transformer.StartTransformer()
	extractor.Start()

	api.Start()

	global.WaitShutdownSig()
}
