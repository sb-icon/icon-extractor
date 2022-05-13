package service

import (
	"github.com/stretchr/testify/require"
	"github.com/sudoblockio/icon-transformer/config"
	"testing"
)

var getLastBlockPayload string = `{
    "jsonrpc": "2.0",
    "method": "icx_getLastBlock",
    "id": 1234
	}`

func TestJsonRpcRequestWithRetry(t *testing.T) {
	config.ReadEnvironment()
	body, err := JsonRpcRequestWithRetry(getLastBlockPayload)
	require.Nil(t, err)
	require.NotEmpty(t, body)
}

func TestJsonRpcRequestWithBackup(t *testing.T) {
	config.ReadEnvironment()
	body, err := JsonRpcRequestWithBackup(getLastBlockPayload)
	require.Nil(t, err)
	require.NotEmpty(t, body)
}

func TestJsonRpcRequestError(t *testing.T) {
	config.ReadEnvironment()
	_, err := JsonRpcRequest(getLastBlockPayload, "http://funky.wrong")
	require.NotEmpty(t, err)
}
