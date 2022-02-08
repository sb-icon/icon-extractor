package extractor

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/utils"
)

type Extractor struct {
	blockNumberQueue  chan int64
	blockNumberCommit chan int64
}

func (e Extractor) Start() {

	go e.start()
}

func (e Extractor) start() {

	for {

		// Wait for block numbers
		blockNumber := <-e.blockNumberQueue

		// Loop until success
		// NOTE break on success
		// NOTE continue on failure
		for {

			/////////////////////////
			// Sent to transformer //
			/////////////////////////
			extractedObjects := []interface{}{}

			///////////////
			// Get block //
			///////////////
			blockRaw, err := utils.IconNodeServiceGetBlockByHeight(blockNumber)
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Get Block, ",
					"BlockNumber=", blockNumber, ", ",
					"Error=", err.Error(),
					" - Retrying in 1 second...",
				)

				time.Sleep(1 * time.Second)
				continue
			}
			extractedObjects = append(extractedObjects, blockRaw)

			////////////////////////////////
			// Extract transaction hashes //
			////////////////////////////////
			transactionHashes, err := utils.IconNodeServiceExtractTransactionHashesFromBlock(blockRaw)
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Extract transaction hashes, ",
					"BlockNumber=", blockNumber, ", ",
					"Error=", err.Error(),
					" - Retrying in 1 second...",
				)

				time.Sleep(1 * time.Second)
				continue
			}

			//////////////////////
			// Get transactions //
			//////////////////////
			for _, transactionHash := range transactionHashes {
				transactionRaw, err := utils.IconNodeServiceGetTransactionByHash(transactionHash)
				if err != nil {
					break
				}

				extractedObjects = append(extractedObjects, transactionRaw)
			}
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Get Transactions, ",
					"BlockNumber=", blockNumber, ", ",
					"TransactionHashes=", transactionHashes, ", ",
					"Error=", err.Error(),
					" - Retrying in 1 second...",
				)

				time.Sleep(1 * time.Second)
				continue
			}

			/////////////////////////
			// Send to transformer //
			/////////////////////////
			// TODO
			zap.S().Info(extractedObjects)

			// Success
			break
		}

		// Commit block number
		e.blockNumberCommit <- blockNumber
	}
}
