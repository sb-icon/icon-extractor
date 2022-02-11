package transformer

import (
	"encoding/json"
	"strconv"

	"github.com/geometry-labs/icon-go-etl/models"
	"github.com/geometry-labs/icon-go-etl/service"
	"go.uber.org/zap"
)

var RawBlockChannel chan service.IconNodeResponseGetBlockByHeight

func StartTransformer() {

	RawBlockChannel = make(chan service.IconNodeResponseGetBlockByHeight)

	go startTransformer()
}

func startTransformer() {

	for {

		///////////////
		// Raw Block //
		///////////////
		rawBlock := <-RawBlockChannel
		block := models.BlockETL{}

		/////////////////
		// Parse Block //
		/////////////////

		// Number
		block.Number = rawBlock.Height

		// Hash
		block.Hash = rawBlock.BlockHash

		// Parent Hash
		block.ParentHash = rawBlock.PrevBlockHash

		// Merkle Root Hash
		block.MerkleRootHash = rawBlock.MerkleTreeRootHash

		// Peer ID
		block.PeerId = rawBlock.PeerId

		// Signature
		block.Signature = rawBlock.Signature

		// Timestamp
		block.Timestamp = rawBlock.Timestamp

		// Version
		block.Version = rawBlock.Version

		////////////////////////
		// Parse Transactions //
		////////////////////////

		// NOTE Some transaction data is in the block struct
		// NOTE This assumes that the transactions in the block
		// struct are in the same order as the transactions array
		for iT, rawTransaction := range rawBlock.ConfirmedTransactionList {
			block.Transactions = append(block.Transactions, &models.TransactionETL{})

			// Hash
			block.Transactions[iT].Hash = rawTransaction.TransactionReceipt.TxHash

			// Timestamp
			if rawTransaction.Timestamp != "" {
				transactionTimestamp, _ := strconv.ParseInt(rawTransaction.Timestamp[2:], 16, 64)
				block.Transactions[iT].Timestamp = transactionTimestamp
			}

			// Transaction Index
			if rawTransaction.TransactionReceipt.TxIndex != "" {
				transactionIndex, _ := strconv.ParseInt(rawTransaction.TransactionReceipt.TxIndex[2:], 16, 64)
				block.Transactions[iT].TransactionIndex = transactionIndex
			}

			// Nonce
			block.Transactions[iT].Nonce = rawTransaction.Nonce

			// Nid
			block.Transactions[iT].Nid = rawTransaction.Nid

			// From Address
			block.Transactions[iT].FromAddress = rawTransaction.FromAddress

			// To Address
			block.Transactions[iT].ToAddress = rawTransaction.ToAddress

			// Value
			// NOTE leave value as string, hex can get really large
			block.Transactions[iT].Value = rawTransaction.Value

			// Status
			block.Transactions[iT].Status = rawTransaction.TransactionReceipt.Status

			// Step Price
			block.Transactions[iT].StepPrice = rawTransaction.TransactionReceipt.StepPrice

			// Step Used
			block.Transactions[iT].StepUsed = rawTransaction.TransactionReceipt.StepUsed

			// Step Limit
			block.Transactions[iT].StepLimit = rawTransaction.StepLimit

			// Step Limit
			block.Transactions[iT].CumulativeStepUsed = rawTransaction.TransactionReceipt.CumulativeStepUsed

			// Logs Bloom
			block.Transactions[iT].LogsBloom = rawTransaction.TransactionReceipt.LogsBloom

			// Data
			if rawTransaction.Data != nil {
				transactionDataString, _ := json.Marshal(&rawTransaction.Data)
				block.Transactions[iT].Data = string(transactionDataString)
			}

			// Data Type
			block.Transactions[iT].DataType = rawTransaction.DataType

			// Score Address
			block.Transactions[iT].ScoreAddress = rawTransaction.TransactionReceipt.ToAddress

			// Signature
			block.Transactions[iT].Signature = rawTransaction.Signature

			// Version
			block.Transactions[iT].Version = rawTransaction.Version

			////////////////
			// Parse Logs //
			////////////////
			for iL, rawLog := range rawTransaction.TransactionReceipt.EventLogs {
				block.Transactions[iT].Logs = append(block.Transactions[iT].Logs, &models.LogETL{})

				// Address
				block.Transactions[iT].Logs[iL].Address = rawLog.ScoreAddress

				// Indexed
				block.Transactions[iT].Logs[iL].Indexed = rawLog.Indexed

				// Data
				block.Transactions[iT].Logs[iL].Data = rawLog.Data
			}
		}

		/////////////////
		// Verify Data //
		/////////////////
		// TODO look into proto field assertions
		// TODO DLQ

		///////////////////
		// Send to Kafka //
		///////////////////
		// TODO
		b, _ := json.Marshal(&block)
		zap.S().Info(string(b))
	}
}
