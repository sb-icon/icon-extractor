package extractor

import "go.uber.org/zap"

func StartManager() {

	go startManager()
}

func startManager() {

	extractorQueueChannel := make(chan int64)
	extractorCommitChannel := make(chan int64)

	extractor := Extractor{
		blockNumberQueue:  extractorQueueChannel,
		blockNumberCommit: extractorCommitChannel,
	}
	extractor.Start()

	blockNumber := int64(1)
	for {
		extractorQueueChannel <- blockNumber

		commitBlockNumber := <-extractorCommitChannel
		zap.S().Info("COMMIT BLOCK #", commitBlockNumber)

		blockNumber++
	}
}
