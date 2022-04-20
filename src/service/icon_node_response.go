package service

//////////////////////
// GetBlockByHeight //
//////////////////////
type IconNodeResponseGetBlockByHeight struct {
	Result *IconNodeResponseGetBlockByHeightResult `json:"result"`
	Error  *IconNodeResponseError                  `json:"error"`
}

type IconNodeResponseGetBlockByHeightResult struct {
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
	Data               interface{}                                `json:"data"` // Data can be int or string. transformer will handle this
	DataType           string                                     `json:"dataType"`
	Timestamp          interface{}                                `json:"timestamp"` // Timestamp can be float64 or string. transformer will handle this
	TxHashV1           string                                     `json:"tx_hash"`
	TxHashV3           string                                     `json:"txHash"`
	TxHash             string                                     // Normalize from V1 and V3
	Version            string                                     `json:"version"`
	FromAddress        string                                     `json:"from"`
	ToAddress          string                                     `json:"to"`
	Value              string                                     `json:"value"`
	Nid                string                                     `json:"nid"`
	Nonce              string                                     `json:"nonce"`
	Signature          string                                     `json:"signature"`
	StepLimit          string                                     `json:"stepLimit"`
	TransactionReceipt IconNodeResponseGetTransactionByHashResult // Field comes from the GetTransactionByHash call
}

//////////////////////////
// GetTransactionByHash //
//////////////////////////
type IconNodeResponseGetTransactionByHash struct {
	Result *IconNodeResponseGetTransactionByHashResult `json:"result"`
	Error  *IconNodeResponseError                      `json:"error"`
}

type IconNodeResponseGetTransactionByHashResult struct {
	BlockHash          string                                         `json:"blockHash"`
	BlockHeight        string                                         `json:"blockHeight"`
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

///////////
// Error //
///////////
// Can occur in any body
type IconNodeResponseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
