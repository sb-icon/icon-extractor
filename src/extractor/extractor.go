package extractor

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/service"
)

type Extractor struct {
	blockNumberQueue  chan int64
	blockNumberCommit chan int64

	blockOutput chan service.IconNodeResponseGetBlockByHeight
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

			///////////////
			// Get block //
			///////////////
			// NOTE rawBlock is sent to transformer
			rawBlock, err := service.IconNodeServiceGetBlockByHeight(blockNumber)
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

			//////////////////////
			// Get transactions //
			//////////////////////
			for i, transaction := range rawBlock.ConfirmedTransactionList {
				transactionHash := ""
				if transaction.TxHashV1 != "" {
					transactionHash = "0x" + transaction.TxHashV1
				} else if transaction.TxHashV3 != "" {
					transactionHash = transaction.TxHashV3
				}

				var transactionRaw *service.IconNodeResponseGetTransactionByHash
				transactionRaw, err = service.IconNodeServiceGetTransactionByHash(transactionHash)
				if err != nil {
					break
				}

				rawBlock.ConfirmedTransactionList[i].TransactionReceipt = *transactionRaw
			}
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Get Transactions, ",
					"BlockNumber=", blockNumber, ", ",
					"Error=", err.Error(),
					" - Retrying in 1 second...",
				)

				time.Sleep(1 * time.Second)
				continue
			}

			/////////////////////////
			// Send to transformer //
			/////////////////////////
			e.blockOutput <- *rawBlock

			// Success
			break
		}

		// Commit block number
		e.blockNumberCommit <- blockNumber
	}
}
