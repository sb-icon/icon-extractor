package extractor

import (
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/crud"
	"github.com/geometry-labs/icon-go-etl/service"
	"github.com/geometry-labs/icon-go-etl/transformer"
)

func Start() {
	// NOTE must start after tranformer is started

	for i := 0; i < config.Config.NumExtractors; i++ {
		e := Extractor{
			transformer.RawBlockChannel,
		}

		e.Start()
	}
}

type Extractor struct {
	blockOutput chan service.IconNodeResponseGetBlockByHeightResult // Output
}

func (e Extractor) Start() {

	go e.start()
}

func (e Extractor) start() {

	// Loop forever, read claim
	for {

		///////////////
		// Get claim //
		///////////////
		claim, err := crud.GetClaimCrud().SelectOneClaim()
		if err != nil {
			zap.S().Warn(
				"Routine=", "Extractor, ",
				"Step=", "Get claim, ",
				"Error=", err.Error(),
				" - Sleeping 1 second...",
			)
			time.Sleep(1 * time.Second)
			continue
		}
		if claim.StartBlockNumber > claim.EndBlockNumber {
			zap.S().Warn(
				"Routine=", "Extractor, ",
				"Step=", "Get claim, ",
				" - Start block number greater than end block number...skipping claim",
			)
			continue
		}

		blockNumberQueue := make([]int64, claim.EndBlockNumber-claim.StartBlockNumber)
		for iB := range blockNumberQueue {
			blockNumberQueue[iB] = claim.StartBlockNumber + int64(iB)
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
				if claim.IsHead == true {
					// keep going
					blockNumberQueue = append(blockNumberQueue, claim.EndBlockNumber)
					claim.EndBlockNumber++
				} else {
					// Done with claim
					break
				}
			}
		}

		//////////////////
		// Commit claim //
		//////////////////
		// Retry until success
		for {
			err = crud.GetClaimCrud().UpdateOneComplete(claim)
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor, ",
					"Step=", "Commit claim, ",
					"Error=", err.Error(),
					" - Sleeping 1 second...",
				)
				time.Sleep(1 * time.Second)
				continue
			}

			// Success
			break
		}
	}
}
