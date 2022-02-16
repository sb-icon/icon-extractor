package extractor

import (
	"github.com/geometry-labs/icon-go-etl/transformer"
)

func StartManager() {

	go startManager(40000000)
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
	for {
		jobQueueChannel <- ExtractorJob{
			startBlockNumber: blockNumber + i,
			endBlockNumber:   blockNumber + i + 100,
		}

		_ = <-jobCommitChannel

		blockNumber += 100
		break
	}
}
