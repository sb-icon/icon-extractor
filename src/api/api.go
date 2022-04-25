package api

import (
	"errors"
	"math"
	"time"

	swagger "github.com/arsmn/fiber-swagger/v2"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"gorm.io/gorm"

	_ "github.com/sudoblockio/icon-extractor/api/docs" // import for swagger docs
	"github.com/sudoblockio/icon-extractor/api/routes"
	"github.com/sudoblockio/icon-extractor/config"
	"github.com/sudoblockio/icon-extractor/crud"
	"github.com/sudoblockio/icon-extractor/models"
)

// @title Go api template docs
// @version 2.0
// @description This is a sample server server.
func Start() {

	app := fiber.New()

	// CORS Middleware
	app.Use(cors.New(cors.Config{
		AllowOrigins:  config.Config.CORSAllowOrigins,
		AllowHeaders:  config.Config.CORSAllowHeaders,
		AllowMethods:  config.Config.CORSAllowMethods,
		ExposeHeaders: config.Config.CORSExposeHeaders,
	}))

	// Swagger docs
	app.Get(config.Config.APIPrefix+"/docs/*", swagger.Handler)

	// Add handlers
	routes.AddHandlers(app)

	go app.Listen(":" + config.Config.APIPort)

	// Insert job if env configured
	if config.Config.InsertExtractorJob == true {

		// Check if job already exist
		_, err := crud.GetJobCrud().SelectOne(config.Config.InsertExtractorJobHash)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			////////////////
			// Create job //
			////////////////
			job := &models.Job{}
			job.StartBlockNumber = int64(config.Config.InsertExtractorJobStartBlockNumber)
			job.EndBlockNumber = int64(config.Config.InsertExtractorJobEndBlockNumber)
			job.CreatedTimestamp = time.Now().Unix()
			job.NumClaims = int64(math.Ceil(float64(config.Config.InsertExtractorJobEndBlockNumber-config.Config.InsertExtractorJobStartBlockNumber) / float64(config.Config.MaxClaimSize)))

			// Hash jobs
			// Set Hash to default
			// Removes duplicates
			job.Hash = config.Config.InsertExtractorJobHash

			// Insert to DB
			crud.GetJobCrud().LoaderChannel <- job

			// Create claims
			for i := 0; i < int(job.NumClaims); i++ {
				claim := &models.Claim{}

				claim.JobHash = job.Hash
				claim.ClaimIndex = int64(i)
				claim.StartBlockNumber = job.StartBlockNumber + int64(config.Config.MaxClaimSize*i)
				claim.EndBlockNumber = claim.StartBlockNumber + int64(config.Config.MaxClaimSize)
				claim.IsClaimed = false
				claim.IsCompleted = false

				// Last claim should not exceed job.EndBlockNumber
				if i == int(job.NumClaims)-1 {
					claim.EndBlockNumber = job.EndBlockNumber
				}

				// Insert to DB
				crud.GetClaimCrud().LoaderChannel <- claim
			}
		}
	}
}
