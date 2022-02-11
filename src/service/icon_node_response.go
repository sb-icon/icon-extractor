package service

//////////////////////
// GetBlockByHeight //
//////////////////////
type IconNodeResponseGetBlockByHeightBody struct {
	Result *IconNodeResponseGetBlockByHeight `json:"result"`
}

type IconNodeResponseGetBlockByHeight struct {
	BlockHash                string                                        `json:"block_hash"`
	ConfirmedTransactionList []IconNodeResponseGetBlockByHeightTransaction `json:"confirmed_transaction_list"`
	Height                   int64                                         `json:"height"`
	MerkleTreeRootHash       string                                        `json:"merkle_tree_root_hash"`
	PeerId                   string                                        `json:"peer_id"`
	PrevBlockHash            string                                        `json:"prev_block_hash"`
	Signature                string                                        `json:"signature"`
	Timestamp                int64                                         `json:"time_stamp"`
	Version                  string                                        `json:"version"`
}

type IconNodeResponseGetBlockByHeightTransaction struct {
	Data               interface{}                          `json:"data"`
	DataType           string                               `json:"dataType"`
	Timestamp          string                               `json:"timestamp"`
	TxHashV1           string                               `json:"tx_hash"`
	TxHashV3           string                               `json:"txHash"`
	Version            string                               `json:"version"`
	FromAddress        string                               `json:"from"`
	ToAddress          string                               `json:"to"`
	Value              string                               `json:"value"`
	Nid                string                               `json:"nid"`
	Nonce              string                               `json:"nonce"`
	Signature          string                               `json:"signature"`
	StepLimit          string                               `json:"stepLimit"`
	TransactionReceipt IconNodeResponseGetTransactionByHash // Field comes from the GetTransactionByHash call
}

//////////////////////////
// GetTransactionByHash //
//////////////////////////
type IconNodeResponseGetTransactionByHashBody struct {
	Result *IconNodeResponseGetTransactionByHash `json:"result"`
}

type IconNodeResponseGetTransactionByHash struct {
	BlockHash          string                                         `json:"blockHash"`
	blockHeight        string                                         `json:"blockHeight"`
	CumulativeStepUsed string                                         `json:"cumulativeStepUsed"`
	EventLogs          []IconNodeResponseGetTransactionByHashEventLog `json:"eventLogs"`
	LogsBloom          string                                         `json:"logsBloom"`
	Status             string                                         `json:"status"`
	StepPrice          string                                         `json:"stepPrice"`
	StepUsed           string                                         `json:"stepUsed"`
	ToAddress          string                                         `json:"to"`
	TxHash             string                                         `json:"txHash"`
	TxIndex            string                                         `json:"txIndex"`
}

type IconNodeResponseGetTransactionByHashEventLog struct {
	ScoreAddress string   `json:"scoreAddress"`
	Indexed      []string `json:"indexed"`
	Data         []string `json:"data"`
}
