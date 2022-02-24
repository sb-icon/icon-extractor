package extractor

import (
	"testing"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/logging"
	"github.com/geometry-labs/icon-go-etl/service"
	"github.com/stretchr/testify/assert"
)

// Runs before every test
func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestStart(t *testing.T) {
	assert := assert.New(t)

	blockOutputChannel := make(chan service.IconNodeResponseGetBlockByHeightResult)

	e := Extractor{
		blockOutputChannel,
	}
	e.Start(true)

	block := <-blockOutputChannel

	// Assert values in block
	assert.NotEqual(0, block.Height)
}
