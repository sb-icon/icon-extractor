package extractor

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/sudoblockio/icon-extractor/config"
	"github.com/sudoblockio/icon-extractor/crud"
	"github.com/sudoblockio/icon-extractor/models"
	"github.com/sudoblockio/icon-extractor/service"
	"github.com/sudoblockio/icon-extractor/transformer"
)

func Start() {
	// NOTE must start after tranformer is started in main.go

	// Claim extractors
	if config.Config.StartClaimExtractors == true {
		zap.S().Info(
			"Routine=", "Extractor",
			", Step=", "Start claim extractors",
			" - Starting claim extractors...",
		)
		for i := 0; i < config.Config.NumClaimExtractors; i++ {
			e := Extractor{
				transformer.RawBlockChannel,
			}

			//e.Start(false)
			go e.start(false)
		}
	}

	// Head extractor
	if config.Config.StartHeadExtractor == true {
		zap.S().Info(
			"Routine=", "Extractor",
			", Step=", "Start head extractor",
			" - Starting head extractor...",
		)
		e := Extractor{
			transformer.RawBlockChannel,
		}
		go e.start(true)
	}
}

type Extractor struct {
	blockOutput chan service.IconNodeResponseGetBlockByHeightResult // Output
}

func (e Extractor) start(isHead bool) {

	// Loop forever, fill queue
	for {
		var blockNumberQueue []int64
		var claim *models.Claim
		var err error

		///////////////////////////////
		// Create block number queue //
		///////////////////////////////
		if isHead == true {
			// Head extractor
			claim, err = crud.GetClaimCrud().SelectOneClaimHead()
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// create claim
				claim = &models.Claim{}
				claim.JobHash = "HEAD_CLAIM"
				claim.ClaimIndex = 0
				claim.StartBlockNumber = int64(config.Config.HeadExtractorStartBlock)
				claim.EndBlockNumber = int64(config.Config.HeadExtractorStartBlock + config.Config.MaxClaimSize)
				claim.IsClaimed = false   // NOTE head claims are never claimed by one extractor
				claim.IsCompleted = false // NOTE head claims are never completed
				claim.IsHead = true

				crud.GetClaimCrud().LoaderChannel <- claim
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			blockNumberQueue = make([]int64, claim.EndBlockNumber-claim.StartBlockNumber)
			for iB := range blockNumberQueue {
				blockNumberQueue[iB] = claim.StartBlockNumber + int64(iB)
			}
		} else {
			// Claim extractor
			claim, err = crud.GetClaimCrud().SelectOneClaim()
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor",
					", Step=", "Get claim",
					", Error=", err.Error(),
					" - Sleeping 1 second...",
				)
				time.Sleep(1 * time.Second)
				continue
			}
			if claim.StartBlockNumber > claim.EndBlockNumber {
				zap.S().Warn(
					"Routine=", "Extractor",
					", Step=", "Get claim",
					" - Start block number greater than end block number...skipping claim",
				)
				continue
			}

			blockNumberQueue = make([]int64, claim.EndBlockNumber-claim.StartBlockNumber)
			for iB := range blockNumberQueue {
				blockNumberQueue[iB] = claim.StartBlockNumber + int64(iB)
			}
		}

		zap.S().Info(
			"Routine=", "Extractor",
			", Step=", "Starting claim",
			", Job Hash=", claim.JobHash,
			", Claim Index=", claim.ClaimIndex,
			", Start=", claim.StartBlockNumber,
			", End=", claim.EndBlockNumber,
			" - Starting claim",
		)

		// Loop through block numbers in queue
		for {
			// Handles case when at end of block range and batch size is more than number of blocks left to process
			batchSize := int(math.Min(float64(config.Config.IconNodeServiceMaxBatchSize), float64(len(blockNumberQueue))))

			blockNumbers := blockNumberQueue[0:batchSize]

			zap.S().Debug(
				"Routine=", "Extractor",
				", Step=", "Create batch numbers",
				", BlockNumbers=", blockNumbers,
			)

			////////////////
			// Get blocks //
			////////////////
			rawBlocksAll, err := service.IconNodeServiceGetBlockByHeight(blockNumbers)
			if err != nil {
				zap.S().Warn(
					"Routine=", "Extractor",
					", Step=", "Get Blocks",
					", BlockNumbers=", blockNumbers,
					", Error=", err.Error(),
				)
				continue
			}

			zap.S().Debug(
				"Routine=", "Extractor",
				", Step=", "Get blocks",
				", Num Blocks=", len(rawBlocksAll),
			)

			//////////////////
			// Check blocks //
			//////////////////
			rawBlocks := []service.IconNodeResponseGetBlockByHeightResult{}
			waitingBlockNumber := int64(-1)
			for iB, block := range rawBlocksAll {
				if block.Error != nil {
					// Error getting block, send block number back to queue
					blockNumberQueue = append(blockNumberQueue, blockNumbers[iB])

					// Error happens when request is valid but block is not created yet
					if block.Error.Code == -31004 {
						// Checks if waitingBlockNumber has been toggled and if it hasn't, initialize it
						// Or check if it has been set, is the waiting block number lower than what has been set
						// This triggers a sleep so that we don't blast our node when we're at the head
						if waitingBlockNumber == -1 || waitingBlockNumber > blockNumbers[iB] {
							waitingBlockNumber = blockNumbers[iB]
						}
					} else {
						zap.S().Warn(
							"Routine=", "Extractor",
							", Step=", "Check block",
							", BlockNumber=", blockNumbers[iB],
							", ErrorCode=", block.Error.Code,
							", ErrorMessage=", block.Error.Message,
						)
						// Sleep a bit if we get an error
						time.Sleep(10 * time.Millisecond)
					}
				} else {
					// Success
					rawBlocks = append(rawBlocks, *block.Result)
				}
			}
			if waitingBlockNumber != -1 {
				zap.S().Info(
					"Routine=", "Extractor",
					", Step=", "Check block",
					", BlockNumber=", waitingBlockNumber,
					" - Waiting for block to be created...",
				)
				time.Sleep(250 * time.Millisecond)
			}

			zap.S().Debug(
				"Routine=", "Extractor",
				", Step=", "Checked blocks",
			)

			//////////////////////
			// Get transactions //
			//////////////////////
			transactionHashQueue := []string{}
			for iB, block := range rawBlocks {

				for iT, transaction := range block.ConfirmedTransactionList {
					// normalize
					hash := ""
					// Hash formatting changed over time v1 / v2
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
						"Routine=", "Extractor",
						", Step=", "Get Transactions",
						", TransactionHashes=", transactionHashes,
						", Error=", err.Error(),
					)
					continue
				}

				zap.S().Debug(
					"Routine=", "Extractor",
					", Step=", "Get transactions",
					", Num Transactions=", len(rawTransactionsAll),
				)

				////////////////////////
				// Check transactions //
				////////////////////////
				rawTransactions := []service.IconNodeResponseGetTransactionByHashResult{}
				waitingTransactionHash := ""
				for iT, transaction := range rawTransactionsAll {
					if transaction.Error != nil {
						// Error getting transaction, send transaction hash back to queue
						transactionHashQueue = append(transactionHashQueue, transactionHashes[iT])

						// Checks if we are at head.  Same sleep logic as what is in block but this time for Txs
						// Trigger sleep if we are at head
						if transaction.Error.Code == -31004 || transaction.Error.Code == -31003 {
							waitingTransactionHash = transactionHashes[iT]
						} else {

							zap.S().Warn(
								"Routine=", "Extractor",
								", Step=", "Check transaction",
								", TransactionHash=", transactionHashes[iT],
								", ErrorCode=", transaction.Error.Code,
								", ErrorMessage=", transaction.Error.Message,
							)
							// Sleep a bit if we get an error
							time.Sleep(10 * time.Millisecond)
						}
					} else {
						// Success
						rawTransactions = append(rawTransactions, *transaction.Result)
					}
				}
				if waitingTransactionHash != "" {
					zap.S().Info(
						"Routine=", "Extractor",
						", Step=", "Check transaction",
						", BlockNumber=", waitingTransactionHash,
						" - Waiting for transaction to be created...",
					)
					time.Sleep(100 * time.Millisecond)
				}

				zap.S().Debug(
					"Routine=", "Extractor",
					", Step=", "Checked transactions",
				)

				////////////////////////////////
				// Add transactions to blocks //
				////////////////////////////////
				for _, transaction := range rawTransactions {

					// Parse block number (Hex)
					transactionBlockNumber, err := strconv.ParseInt(strings.Replace(transaction.BlockHeight, "0x", "", -1), 16, 64)
					if err != nil {
						zap.S().Warn(
							"Routine=", "Extractor",
							", Step=", "Parse Transaction Block Number",
							", TransactionHash=", transaction.TxHash,
							", TransactionBlockNumber=", transactionBlockNumber,
							", Error=", err.Error(),
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

			zap.S().Debug(
				"Routine=", "Extractor",
				", Step=", "Send blocks to transformer",
			)

			/////////////////
			// Check queue //
			/////////////////
			blockNumberQueue = blockNumberQueue[batchSize:]
			if len(blockNumberQueue) == 0 {
				if isHead == true {
					// Head extractor
					zap.S().Info(
						"Routine=", "Extractor",
						", Step=", "Finished claim",
						", Job Hash=", claim.JobHash,
						", Claim Index=", claim.ClaimIndex,
						", Start=", claim.StartBlockNumber,
						", End=", claim.EndBlockNumber,
						" - Finished claim",
					)

					// Add next block to queue
					claim.StartBlockNumber = claim.EndBlockNumber
					claim.EndBlockNumber = claim.StartBlockNumber + int64(config.Config.MaxClaimSize)

					blockNumberQueue = make([]int64, claim.EndBlockNumber-claim.StartBlockNumber)
					for iB := range blockNumberQueue {
						blockNumberQueue[iB] = claim.StartBlockNumber + int64(iB)
					}

					// commit to postgres
					crud.GetClaimCrud().LoaderChannel <- claim
				} else {
					// Claim extracted
					// Done with claim
					zap.S().Info(
						"Routine=", "Extractor",
						", Step=", "Finished claim",
						", Job Hash=", claim.JobHash,
						", Claim Index=", claim.ClaimIndex,
						", Start=", claim.StartBlockNumber,
						", End=", claim.EndBlockNumber,
						" - Finished claim",
					)
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
					"Routine=", "Extractor",
					", Step=", "Commit claim",
					", Error=", err.Error(),
					" - Sleeping 1 second...",
				)
				time.Sleep(1 * time.Second)
				continue
			}

			// Success
			break
		}
	}

	zap.S().Debug(
		"Routine=", "Extractor",
		", Step=", "Exiting",
		" - Exiting extractor",
	)
}
