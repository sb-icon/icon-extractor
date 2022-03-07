package extractor

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/sudoblockio/icon-go-etl/config"
	"github.com/sudoblockio/icon-go-etl/crud"
	"github.com/sudoblockio/icon-go-etl/models"
	"github.com/sudoblockio/icon-go-etl/service"
	"github.com/sudoblockio/icon-go-etl/transformer"
)

func Start() {
	// NOTE must start after tranformer is started

	// Claim extractors
	if config.Config.StartClaimExtractors == true {
		for i := 0; i < config.Config.NumClaimExtractors; i++ {
			e := Extractor{
				transformer.RawBlockChannel,
			}

			e.Start(false)
		}
	}

	// Head extractor
	if config.Config.StartHeadExtractor == true {
		e := Extractor{
			transformer.RawBlockChannel,
		}
		e.Start(true)
	}
}

type Extractor struct {
	blockOutput chan service.IconNodeResponseGetBlockByHeightResult // Output
}

func (e Extractor) Start(isHead bool) {

	go e.start(isHead)
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
				claim.EndBlockNumber = int64(config.Config.HeadExtractorStartBlock)
				claim.IsClaimed = false   // NOTE head claims are never claimed by one extractor
				claim.IsCompleted = false // NOTE head claims are never completed
				claim.IsHead = true

				crud.GetClaimCrud().LoaderChannel <- claim
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			blockNumberQueue = make([]int64, 1)
			blockNumberQueue[0] = claim.StartBlockNumber
		} else {
			// Claim extractor
			claim, err = crud.GetClaimCrud().SelectOneClaim()
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

			blockNumberQueue = make([]int64, claim.EndBlockNumber-claim.StartBlockNumber)
			for iB := range blockNumberQueue {
				blockNumberQueue[iB] = claim.StartBlockNumber + int64(iB)
			}
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
				if isHead == true {
					// Head extractor
					// Add next block to queue
					blockNumberQueue = append(blockNumberQueue, claim.EndBlockNumber)
					claim.StartBlockNumber++
					claim.EndBlockNumber++

					// commit to postgres
					crud.GetClaimCrud().LoaderChannel <- claim
				} else {
					// Claim extracto
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
