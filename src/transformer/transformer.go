package transformer

import (
	"encoding/json"

	"github.com/geometry-labs/icon-go-etl/service"
	"go.uber.org/zap"
)

type RawMessage struct {
	Block        service.IconNodeResponseGetBlockByHeight
	Transactions []interface{}
}

var RawMessageChannel chan RawMessage

func StartTransformer() {

	RawMessageChannel = make(chan RawMessage)

	go startTransformer()
}

func startTransformer() {

	for {

		/////////////////
		// Raw Message //
		/////////////////
		rawMessage := <-RawMessageChannel

		// block := models.BlockETL{}

		/*
			/////////////////
			// Parse Block //
			/////////////////
			rawBlock, _ := rawMessage.Block.(map[string]interface{})

			// Number
			if blockNumber, ok := rawBlock["height"]; ok {
				block.Number, _ = int64(blockNumber.(float64))
			}

			// Hash
			if blockHash, ok := rawBlock["block_hash"]; ok {
				block.Hash, _ = blockHash.(string)
			}

			// Parent Hash
			if blockParentHash, ok := rawBlock["prev_block_hash"]; ok {
				block.ParentHash, _ = blockParentHash.(string)
			}

			// Merkle Root Hash
			if blockMerkleRootHash, ok := rawBlock["merkle_tree_root_hash"]; ok {
				block.MerkleRootHash, _ = blockMerkleRootHash.(string)
			}

			// Peer ID
			if blockPeerId, ok := rawBlock["peer_id"]; ok {
				block.PeerId, _ = blockPeerId.(string)
			}

			// Signature
			if blockSignature, ok := rawBlock["signature"]; ok {
				block.Signature, _ = blockSignature.(string)
			}

			// Timestamp
			if blockTimestamp, ok := rawBlock["time_stamp"]; ok {
				block.Timestamp = int64(blockTimestamp.(float64))
			}

			// Version
			if blockVersion, ok := rawBlock["version"]; ok {
				block.Version, _ = blockVersion.(string)
			}

			////////////////////////
			// Parse Transactions //
			////////////////////////

			// NOTE Some transaction data is in the block struct
			// NOTE This assumes that the transactions in the block
			// struct are in the same order as the transactions array
			var rawBlockTransactions []map[string]interface{}
			if rawTXs, ok := rawBlock["confirmed_transaction_list"]; ok {
				rawBlockTXs, ok := rawTXs.([]interface{})
				if ok == false {
					// TODO DLQ
				}

				for _, rawTX := range rawBlockTXs {
					rawBlockTransaction, ok := rawTX.(map[string]interface{})
					if ok == false {
						continue
					}
					rawBlockTransactions = append(rawBlockTransactions, rawBlockTransaction)
				}

				if len(rawBlockTransactions) != len(rawMessage.Transactions) {
					// TODO DLQ
				}
			}

			for _, rawTransactionInterface := range rawMessage.Transactions {
				rawTransaction, _ := rawTransactionInterface.(map[string]interface{})

				block.Transactions = append(block.Transactions, models.TransactionETL{})
				i := len(block.Transactions) - 1

				// Hash
				if transactionHash, ok := rawTransaction["txHash"]; ok {
					block.Transactions[i].Hash = transactionHash.(string)
				}

				// Timestamp
				if transactionTimestamp, ok := rawBlockTransactions[i]["timestamp"]; ok {
					// Hex -> int64
					transactionTimestampString, _ := transactionTimestamp.(string)
					transactionTimestampInt, _ := strconv.ParseInt(transactionTimestampString[2:], 16, 64)

					block.Transactions[i].Timestamp = transactionTimestampInt
				}

				// Transaction Index
				if transactionIndex, ok := rawTransaction["txIndex"]; ok {
					// Hex -> int64
					transactionIndexString, _ := transactionIndex.(string)
					transactionIndexInt, _ := strconv.ParseInt(transactionIndexString[2:], 16, 64)

					block.Transactions[i].TransactionIndex = transactionIndexInt
				}

				// Nonce
				if transactionNonce, ok := rawBlockTransactions[i]["nonce"]; ok {
					block.Transactions[i].Nonce, _ = transactionNonce.(string)
				}

				// Nid
				if transactionNid, ok := rawBlockTransactions[i]["nid"]; ok {
					block.Transactions[i].Nid, _ = transactionNid.(string)
				}

				// From Address
				if transactionFromAddress, ok := rawBlockTransactions[i]["from"]; ok {
					block.Transactions[i].FromAddress, _ = transactionFromAddress.(string)
				}

				// To Address
				if transactionToAddress, ok := rawBlockTransactions[i]["to"]; ok {
					block.Transactions[i].ToAddress, _ = transactionToAddress.(string)
				}

				// Value
				if transactionValue, ok := rawBlockTransactions[i]["value"]; ok {
					block.Transactions[i].Value, _ = transactionValue.(string)
				}

				// Status
				if transactionStatus, ok := rawTransaction["status"]; ok {
					block.Transactions[i].Status, _ = transactionStatus.(string)
				}

				// Step Price
				if transactionStepPrice, ok := rawTransaction["step_price"]; ok {
					block.Transactions[i].StepPrice, _ = transactionStepPrice.(string)
				}

				// Step Used
				if transactionStepUsed, ok := rawTransaction["step_used"]; ok {
					block.Transactions[i].StepUsed, _ = transactionStepUsed.(string)
				}

				// Step Limit
				if transactionStepLimit, ok := rawTransaction["step_limit"]; ok {
					block.Transactions[i].StepLimit, _ = transactionStepLimit.(string)
				}

				// Cumulative Step Used
				if transactionCumulativeStepUsed, ok := rawTransaction["cumulative_step_used"]; ok {
					block.Transactions[i].CumulativeStepUsed, _ = transactionCumulativeStepUsed.(string)
				}

				// Logs Bloom
				if transactionLogsBloom, ok := rawTransaction["logs_bloom"]; ok {
					block.Transactions[i].LogsBloom, _ = transactionLogsBloom.(string)
				}
			}

			////////////////
			// Parse Logs //
			////////////////
			// TODO

			/////////////////
			// Verify Data //
			/////////////////
			// TODO

			///////////////////
			// Send to Kafka //
			///////////////////
			// TODO
		*/
		b, _ := json.Marshal(&rawMessage)
		zap.S().Info(string(b))
	}
}
