package extractor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sudoblockio/icon-go-etl/config"
	"github.com/sudoblockio/icon-go-etl/logging"
	"github.com/sudoblockio/icon-go-etl/service"
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
