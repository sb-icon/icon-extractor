package extractor

import (
	"github.com/geometry-labs/icon-go-etl/transformer"
)

func StartManager() {

	go startManager(46235085)
}

func startManager(blockNumber int64) {

	jobQueueChannel := make(chan ExtractorJob)
	jobCommitChannel := make(chan ExtractorJob)

	extractor := Extractor{
		jobQueue:    jobQueueChannel,
		jobCommit:   jobCommitChannel,
		blockOutput: transformer.RawBlockChannel,
	}
	extractor.Start()

	i := int64(0)
	batchSize := int64(1)
	for {
		jobQueueChannel <- ExtractorJob{
			startBlockNumber: blockNumber + i,
			endBlockNumber:   blockNumber + i + batchSize,
		}

		_ = <-jobCommitChannel

		blockNumber += batchSize
	}
}
