package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sudoblockio/icon-extractor/config"
)

func JsonRpcRequest(payload string, url string) (map[string]interface{}, error) {

	// Request icon contract
	method := "POST"
	resp := map[string]interface{}{}

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return resp, err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return resp, err
	}

	// Check status code
	if res.StatusCode != 200 {
		return resp, errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	err = json.Unmarshal(bodyString, &resp)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func JsonRpcRequestWithBackup(payload string) (map[string]interface{}, error) {
	var err error
	for _, icon_node_url := range config.Config.IconNodeServiceURL {
		resp, err := JsonRpcRequest(payload, icon_node_url)
		if err == nil {
			return resp, err
		}

		if err != nil {
			log.Println("primary icon node rpc err:", err)
		}
	}
	return nil, err
}

func JsonRpcRequestWithRetry(payload string) (map[string]interface{}, error) {
	resp, err := retry(payload, JsonRpcRequestWithBackup)
	return resp, err
}

func retry(payload string, JsonRpcRequestWithBackup func(payload string) (map[string]interface{}, error)) (_ map[string]interface{}, err error) {
	attempts := config.Config.IconNodeRpcRetryAttempts
	duration := config.Config.IconNodeRpcRetrySleepSeconds
	for i := 0; ; i++ {
		resp, err := JsonRpcRequestWithBackup(payload)
		if err == nil {
			return resp, err
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(duration)
		log.Println("retrying after error:", err)
	}
	return map[string]interface{}{}, fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
