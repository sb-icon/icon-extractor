package service

import (
	"testing"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/stretchr/testify/assert"
)

// Runs before every test
func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()
}

func TestIconNodeServiceGetBlockByHeight(t *testing.T) {
	assert := assert.New(t)

	blocks, err := IconNodeServiceGetBlockByHeight([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	assert.Equal(nil, err)
	assert.Equal(10, len(blocks))
}

func TestIconNodeServiceGetTransactionByHash(t *testing.T) {
	assert := assert.New(t)

	hashes := []string{
		"0x375540830d475a73b704cf8dee9fa9eba2798f9d2af1fa55a85482e48daefd3b",
		"0x1b6133792cee1ab2e54ae68faf9f49daf81c7e46d68b1ca281acc718602c77dd",
		"0x9be449059eea12cc4684153f3a16f361a556caf6ab510d20fac7cb6cf46b9e9f",
		"0xdf6ca8625cc0d97cbfc2bf28023a3143644e1c6c7df95ef93a6778a6cdbfdce3",
		"0x3aac6b5103354b273de7ac33b2c832ce69e08e546f1cf32cb7c59e1f27ad4918",
		"0x5b8d95c70da3069b7424c378fa282c802d30e55b6d3db3c464a0b4582b8dbd55",
		"0x13b28686fe7e1e13c54251c0a31e0ad743e83941dd56a6241cd2e2350bae4f35",
		"0x92bf2ecbab8577109a2c61d71496ee9d94c2dae4a46a989a619ad4a5ab0fd3f3",
		"0x59cce5669e63c554701ce4c529ae31f463da7abe76bd408020868ec7ba70e046",
		"0x785caec404c95650352f4018595606fdf339d8c17c57cb04f6e0ce5d15e75b13",
	}

	transactions, err := IconNodeServiceGetTransactionByHash(hashes)
	assert.Equal(nil, err)
	assert.Equal(10, len(transactions))
}
