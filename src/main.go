package main

import (
	"github.com/sb-icon/icon-extractor/api"
	"github.com/sb-icon/icon-extractor/config"
	"github.com/sb-icon/icon-extractor/extractor"
	"github.com/sb-icon/icon-extractor/global"
	"github.com/sb-icon/icon-extractor/kafka"
	"github.com/sb-icon/icon-extractor/logging"
	"github.com/sb-icon/icon-extractor/transformer"
)

func main() {
	config.ReadEnvironment()

	logging.Init()

	api.Start()

	kafka.StartProducers()
	transformer.StartTransformer() // Need to start before extractor so the RawBlockChannel is opened
	extractor.Start()

	global.WaitShutdownSig()
}
