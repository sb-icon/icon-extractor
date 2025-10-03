package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sb-icon/icon-extractor/config"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func IconNodeServiceGetBlockByHeight(heights []int64) ([]IconNodeResponseGetBlockByHeight, error) {

	blocks := []IconNodeResponseGetBlockByHeight{}

	if len(heights) > config.Config.IconNodeServiceMaxBatchSize {
		return blocks, errors.New(
			"Requested=" + strconv.Itoa(len(heights)) +
				"MaxBatchSize=" + strconv.Itoa(config.Config.IconNodeServiceMaxBatchSize) +
				"Error=Requested blocks is greater that max batch size",
		)
	}
	if len(heights) == 0 {
		return blocks, nil
	}

	// Request icon contract
	url := config.Config.IconNodeServiceURL
	method := "POST"

	// Create json string payload with list of get block requests for batch
	payload := "["
	for i, height := range heights {

		singleRequest := fmt.Sprintf(`{
			"jsonrpc": "2.0",
			"method": "icx_getBlockByHeight",
			"id": 1,
			"params": {
					"height": "0x%x"
			}
		}`, height)

		payload += singleRequest
		if i != len(heights)-1 {
			payload += ","
		}
	}
	payload += "]"

	// Create http client
	client := &http.Client{
		Timeout: config.Config.HttpClientTimeout,
	}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return blocks, err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return blocks, err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return blocks, err
	}

	// Check status code
	if res.StatusCode != 200 {
		// TODO check err code in response body
		return blocks, errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	err = json.Unmarshal(bodyString, &blocks)
	if err != nil {
		return blocks, err
	}

	// Extract result
	return blocks, nil
}

func IconNodeServiceGetTransactionByHash(hashes []string) ([]IconNodeResponseGetTransactionByHash, error) {

	transactions := []IconNodeResponseGetTransactionByHash{}

	if len(hashes) > config.Config.IconNodeServiceMaxBatchSize {
		return transactions, errors.New(
			"Requested=" + strconv.Itoa(len(hashes)) +
				"MaxBatchSize=" + strconv.Itoa(config.Config.IconNodeServiceMaxBatchSize) +
				"Error=Requested transactions is greater that max batch size",
		)
	}
	if len(hashes) == 0 {
		return transactions, nil
	}

	// Request icon contract
	url := config.Config.IconNodeServiceURL
	method := "POST"

	// Create json string payload with list of get transformer requests for batch
	payload := "["
	for i, hash := range hashes {

		singleRequest := fmt.Sprintf(`{
			"jsonrpc": "2.0",
			"method": "icx_getTransactionResult",
			"id": 1,
			"params": {
					"txHash": "%s"
			}
		}`, hash)

		payload += singleRequest
		if i != len(hashes)-1 {
			payload += ","
		}
	}
	payload += "]"

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return transactions, err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return transactions, err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return transactions, err
	}

	// Check status code
	if res.StatusCode != 200 {
		return transactions, errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	err = json.Unmarshal(bodyString, &transactions)
	if err != nil {
		return transactions, err
	}

	// Extract result
	return transactions, nil
}
