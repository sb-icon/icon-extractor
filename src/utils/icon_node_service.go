package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/geometry-labs/icon-go-etl/config"
)

func IconNodeServiceGetBlockByHeight(height int64) (interface{}, error) {

	// Request icon contract
	url := config.Config.IconNodeServiceURL
	method := "POST"
	payload := fmt.Sprintf(`{
    "jsonrpc": "2.0",
    "method": "icx_getBlockByHeight",
    "id": 1,
    "params": {
        "height": "0x%x"
    }
	}`, height)

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// Check status code
	if res.StatusCode != 200 {
		return nil, errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	body := map[string]interface{}{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		return nil, err
	}

	// Extract result
	result, ok := body["result"]
	if ok == false {
		return nil, errors.New("Cannot read result")
	}

	return result, nil
}

func IconNodeServiceExtractTransactionHashesFromBlock(block interface{}) ([]string, error) {

	// Extract result
	result, ok := block.(map[string]interface{})
	if ok == false {
		return nil, errors.New("Cannot read result")
	}

	// Extract transactions
	transactions, ok := result["confirmed_transaction_list"].([]interface{})
	if ok == false {
		return nil, errors.New("Cannot read confirmed_transaction_list")
	}

	// Extract transaciton hashes
	transactionHashes := []string{}
	for _, t := range transactions {
		tx, ok := t.(map[string]interface{})
		if ok == false {
			return nil, errors.New("1 Cannot read transaction hash")
		}

		// V1
		hash, ok := tx["tx_hash"].(string)
		if ok == true {
			transactionHashes = append(transactionHashes, "0x"+hash)
			continue
		}

		// V3
		hash, ok = tx["txHash"].(string)
		if ok == true {
			transactionHashes = append(transactionHashes, hash)
			continue
		}

		if ok == false {
			return nil, errors.New("2 Cannot read transaction hash")
		}
	}

	return transactionHashes, nil
}

func IconNodeServiceGetTransactionByHash(hash string) (interface{}, error) {

	// Request icon contract
	url := config.Config.IconNodeServiceURL
	method := "POST"
	payload := fmt.Sprintf(`{
    "jsonrpc": "2.0",
    "method": "icx_getTransactionResult",
    "id": 1,
    "params": {
        "txHash": "%s"
    }
	}`, hash)

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// Check status code
	if res.StatusCode != 200 {
		return nil, errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	body := map[string]interface{}{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		return nil, err
	}

	// Extract result
	result, ok := body["result"]
	if ok == false {
		return nil, errors.New("Cannot read result")
	}

	return result, nil
}
