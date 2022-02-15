package extractor

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/service"
)

type ExtractorJob struct {
	startBlockNumber int64
	endBlockNumber   int64
}

type Extractor struct {
	jobQueue chan ExtractorJob
	commit   chan int64

	blockOutput chan service.IconNodeResponseGetBlockByHeight
}

func (e Extractor) Start() {

	go e.start()
}

func (e Extractor) start() {

	for {

		// Wait for a job
		extractorJob := <-e.jobQueue

		blockNumbers := []int64{}
		for i := 0; i < config.Config.IconNodeServiceMaxBatchSize; i++ {
			if extractorJob.startBlockNumber+i >= extractorJob.endBlockNumber {
				break
			}

			blockNumbers = append(blockNumbers, extractorJob.startBlockNumber+i)
		}

		// Loop until success
		// NOTE break on success
		// NOTE continue on failure
		for {

			///////////////
			// Get block //
			///////////////
			// NOTE rawBlock is sent to transformer
			rawBlocks, err := service.IconNodeServiceGetBlockByHeight(blockNumbers)
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
