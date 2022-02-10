package extractor

import "go.uber.org/zap"

func StartManager() {

	go startManager(40000000)
}

func startManager(blockNumber int64) {

	extractorQueueChannel := make(chan int64)
	extractorCommitChannel := make(chan int64)

	extractor := Extractor{
		blockNumberQueue:  extractorQueueChannel,
		blockNumberCommit: extractorCommitChannel,
	}
	extractor.Start()

	for {
		extractorQueueChannel <- blockNumber

		commitBlockNumber := <-extractorCommitChannel
		zap.S().Info("COMMIT BLOCK #", commitBlockNumber)

		blockNumber++
	}
}
