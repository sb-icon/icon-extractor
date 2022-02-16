package extractor

import (
	"math"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/service"
)

type Job struct {
	startBlockNumber int64 // Inclusive
	endBlockNumber   int64 // Exclusive
}

type Extractor struct {
	jobQueue    chan Job                                            // Input
	jobCommit   chan Job                                            // Output
	blockOutput chan service.IconNodeResponseGetBlockByHeightResult // Output
}

func (e Extractor) Start() {

	go e.start()
}

func (e Extractor) start() {

	// Loop forever, read job queue
	for {

		// Wait for a job
		job := <-e.jobQueue

		blockNumberQueue := make([]int64, job.endBlockNumber-job.startBlockNumber)
		for iB := range blockNumberQueue {
			blockNumberQueue[iB] = job.startBlockNumber + int64(iB)
		}

		// Loop through block numbers in queue
		for {
			batchSize := int(math.Min(float64(config.Config.IconNodeServiceMaxBatchSize), float64(len(blockNumberQueue))))

			blockNumbers := blockNumberQueue[0:batchSize]

			////////////////
			// Get blocks //
			////////////////
			rawBlocksAll, err := service.IconNodeServiceGetBlockByHeight(blockNumbers)
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Get Blocks, ",
					"BlockNumbers=", blockNumbers, ", ",
					"Error=", err.Error(),
				)
				continue
			}

			//////////////////
			// Check blocks //
			//////////////////
			rawBlocks := []service.IconNodeResponseGetBlockByHeightResult{}
			for iB, block := range rawBlocksAll {
				if block.Error != nil {
					// Error getting block, send block number back to queue
					blockNumberQueue = append(blockNumberQueue, blockNumbers[iB])

					if block.Error.Code == -31004 {
						zap.S().Info(
							"Routine=", "Extractor, ",
							"Step=", "Check block, ",
							"BlockNumber=", blockNumbers[iB], ", ",
							" - Waiting for block to be created...",
						)
					} else {
						zap.S().Warn(
							"Routine=", "Extractor, ",
							"Step=", "Check block, ",
							"BlockNumber=", blockNumbers[iB], ", ",
							"ErrorCode=", block.Error.Code, ", ",
							"ErrorMessage=", block.Error.Message, ", ",
						)
					}
				} else {
					// Success
					rawBlocks = append(rawBlocks, *block.Result)
				}
			}

			//////////////////////
			// Get transactions //
			//////////////////////
			transactionHashQueue := []string{}
			for iB, block := range rawBlocks {

				for iT, transaction := range block.ConfirmedTransactionList {
					// normalize
					hash := ""
					if transaction.TxHashV1 != "" {
						hash = "0x" + transaction.TxHashV1
					} else if transaction.TxHashV3 != "" {
						hash = transaction.TxHashV3
					}
					rawBlocks[iB].ConfirmedTransactionList[iT].TxHash = hash

					// Add to queue
					transactionHashQueue = append(transactionHashQueue, hash)
				}
			}

			// Loop through all transaction hashes in queue
			for {
				batchSize := int(math.Min(float64(config.Config.IconNodeServiceMaxBatchSize), float64(len(transactionHashQueue))))

				transactionHashes := transactionHashQueue[0:batchSize]

				rawTransactionsAll, err := service.IconNodeServiceGetTransactionByHash(transactionHashes)
				if err != nil {
					zap.S().Warn(
						"Routine=", "Extractor, ",
						"Step=", "Get Transactions, ",
						"TransactionHashes=", transactionHashes, ", ",
						"Error=", err.Error(),
					)
					continue
				}

				////////////////////////
				// Check transactions //
				////////////////////////
				rawTransactions := []service.IconNodeResponseGetTransactionByHashResult{}
				for iT, transaction := range rawTransactionsAll {
					if transaction.Error != nil {
						// Error getting transaction, send transaction hash back to queue
						transactionHashQueue = append(transactionHashQueue, transactionHashes[iT])

						if transaction.Error.Code == -31004 || transaction.Error.Code == -31003 {
							zap.S().Info(
								"Routine=", "Extractor, ",
								"Step=", "Check transaction, ",
								"BlockNumber=", transactionHashes[iT], ", ",
								" - Waiting for transaction to be created...",
							)
						} else {

							zap.S().Warn(
								"Routine=", "Extractor, ",
								"Step=", "Check transaction, ",
								"TransactionHash=", transactionHashes[iT], ", ",
								"ErrorCode=", transaction.Error.Code, ", ",
								"ErrorMessage=", transaction.Error.Message, ", ",
							)
						}
					} else {
						// Success
						rawTransactions = append(rawTransactions, *transaction.Result)
					}
				}

				////////////////////////////////
				// Add transactions to blocks //
				////////////////////////////////
				for _, transaction := range rawTransactions {

					// Parse block number (Hex)
					transactionBlockNumber, err := strconv.ParseInt(strings.Replace(transaction.BlockHeight, "0x", "", -1), 16, 64)
					if err != nil {
						zap.S().Warn(
							"Routine=", "Extractor, ",
							"Step=", "Parse Transaction Block Number, ",
							"TransactionHash=", transaction.TxHash, ", ",
							"TransactionBlockNumber=", transactionBlockNumber, ", ",
							"Error=", err.Error(),
						)
						continue
					}

					// Find block
					for iB, block := range rawBlocks {
						if block.Height == transactionBlockNumber {

							// Find transaction
							for iT, blockTransaction := range block.ConfirmedTransactionList {
								if blockTransaction.TxHash == transaction.TxHash {

									// Insert
									rawBlocks[iB].ConfirmedTransactionList[iT].TransactionReceipt = transaction
								}
							}
						}
					}
				}

				/////////////////
				// Check queue //
				/////////////////
				transactionHashQueue = transactionHashQueue[batchSize:]
				if len(transactionHashQueue) == 0 {
					// Done with job
					break
				}
			}

			/////////////////////////
			// Send to transformer //
			/////////////////////////
			for _, block := range rawBlocks {
				e.blockOutput <- block
			}

			/////////////////
			// Check queue //
			/////////////////
			blockNumberQueue = blockNumberQueue[batchSize:]
			if len(blockNumberQueue) == 0 {
				// Done with job
				break
			}
		}

		// Commit job
		e.jobCommit <- job
	}
}
