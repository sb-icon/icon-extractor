package transformer

import (
	"github.com/geometry-labs/icon-go-etl/models"
	"go.uber.org/zap"
)

type RawMessage struct {
	Block        interface{}
	Transactions []interface{}
}

var RawMessageChannel chan RawMessage

func StartTransformer() {

	RawMessageChannel = make(chan RawMessage)

	go startTransformer()
}

func startTransformer() {

	for {

		block := models.BlockETL{}

		/////////////////
		// Raw Message //
		/////////////////

		rawMessage := <-RawMessageChannel

		/////////////////
		// Parse Block //
		/////////////////
		rawBlock, ok := rawMessage.Block.(map[string]interface{})
		if ok == false {
			// TODO
		}

		block.Number = int64(rawBlock["height"].(int))
		block.Hash = rawBlock["block_hash"].(string)
		block.MerkleRootHash = rawBlock["merkle_tree_root_hash"].(string)
		block.PeerId = rawBlock["peer_id"].(string)
		block.Signature = rawBlock["signature"].(string)
		block.Timestamp = int64(rawBlock["time_stamp"].(int))
		block.Version = rawBlock["version"].(string)

		////////////////////////
		// Parse Transactions //
		////////////////////////
		// TODO

		/////////////////
		// Verify Data //
		/////////////////
		// TODO

		///////////////////
		// Send to Kafka //
		///////////////////
		// TODO
		zap.S().Info(block)
	}
}
