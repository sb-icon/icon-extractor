package main

import (
	"github.com/sudoblockio/icon-go-etl/api"
	"github.com/sudoblockio/icon-go-etl/config"
	"github.com/sudoblockio/icon-go-etl/extractor"
	"github.com/sudoblockio/icon-go-etl/global"
	"github.com/sudoblockio/icon-go-etl/kafka"
	"github.com/sudoblockio/icon-go-etl/logging"
	"github.com/sudoblockio/icon-go-etl/transformer"
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
