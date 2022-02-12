package extractor

import (
	"github.com/geometry-labs/icon-go-etl/transformer"
)

func StartManager() {

	go startManager(40000000)
	go startManager(39000000)
	go startManager(38000000)
	go startManager(37000000)
	go startManager(36000000)
	go startManager(35000000)
}

func startManager(blockNumber int64) {

	extractorQueueChannel := make(chan int64)
	extractorCommitChannel := make(chan int64)

	extractor := Extractor{
		blockNumberQueue:  extractorQueueChannel,
		blockNumberCommit: extractorCommitChannel,
		blockOutput:       transformer.RawBlockChannel,
	}
	extractor.Start()

	for {
		extractorQueueChannel <- blockNumber

		_ = <-extractorCommitChannel

		blockNumber++
	}
}
