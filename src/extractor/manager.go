package extractor

import "go.uber.org/zap"

func StartManager() {

	go startManager(1)
	go startManager(40000000)
	go startManager(30000000)
	go startManager(20000000)
	go startManager(10000000)
	go startManager(9000000)
	go startManager(8000000)
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
