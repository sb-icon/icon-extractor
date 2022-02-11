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
		for i, rawTransaction := range rawBlock.ConfirmedTransactionList {
			block.Transactions = append(block.Transactions, &models.TransactionETL{})

			// Hash
			block.Transactions[i].Hash = rawTransaction.TransactionReceipt.TxHash

			// Timestamp
			if rawTransaction.Timestamp != "" {
				transactionTimestamp, _ := strconv.ParseInt(rawTransaction.Timestamp[2:], 16, 64)
				block.Transactions[i].Timestamp = transactionTimestamp
			}

			// Transaction Index
			if rawTransaction.TransactionReceipt.TxIndex != "" {
				transactionIndex, _ := strconv.ParseInt(rawTransaction.TransactionReceipt.TxIndex[2:], 16, 64)
				block.Transactions[i].TransactionIndex = transactionIndex
			}

			// Nonce
			block.Transactions[i].Nonce = rawTransaction.Nonce

			// Nid
			block.Transactions[i].Nid = rawTransaction.Nid

			// From Address
			block.Transactions[i].FromAddress = rawTransaction.FromAddress

			// To Address
			block.Transactions[i].ToAddress = rawTransaction.ToAddress

			// Value
			// NOTE leave value as string, hex can get really large
			block.Transactions[i].Value = rawTransaction.Value

			// Status
			block.Transactions[i].Status = rawTransaction.TransactionReceipt.Status

			// Step Price
			block.Transactions[i].StepPrice = rawTransaction.TransactionReceipt.StepPrice

			// Step Used
			block.Transactions[i].StepUsed = rawTransaction.TransactionReceipt.StepUsed

			// Step Limit
			block.Transactions[i].StepLimit = rawTransaction.StepLimit

			// Step Limit
			block.Transactions[i].CumulativeStepUsed = rawTransaction.TransactionReceipt.CumulativeStepUsed

			// Logs Bloom
			block.Transactions[i].LogsBloom = rawTransaction.TransactionReceipt.LogsBloom

			// Data
			if rawTransaction.Data != nil {
				transactionDataString, _ := json.Marshal(&rawTransaction.Data)
				block.Transactions[i].Data = string(transactionDataString)
			}

			// Data Type
			block.Transactions[i].DataType = rawTransaction.DataType

			// Score Address
			block.Transactions[i].ScoreAddress = rawTransaction.TransactionReceipt.ToAddress

			// Signature
			block.Transactions[i].Signature = rawTransaction.Signature

			// Version
			block.Transactions[i].Version = rawTransaction.Version
		}

		////////////////
		// Parse Logs //
		////////////////
		// TODO

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
