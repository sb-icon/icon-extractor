package routes

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/sudoblockio/icon-extractor/config"
	"github.com/sudoblockio/icon-extractor/crud"
	"github.com/sudoblockio/icon-extractor/models"
)

type JobsBody struct {
	StartBlockNumber int `json:"start_block_number"`
	EndBlockNumber   int `json:"end_block_number"`
}

// BlocksAddHandlers - add blocks endpoints to fiber router
func AddHandlers(app *fiber.App) {

	prefix := config.Config.APIPrefix

	app.Post(prefix+"/create-job", handlerCreateJob)
}

// Create Jobs
// @Summary create an etl job
// @Tags Jobs
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Router /api/v1/create-job [post]
// @Param body body JobsBody true "{}"
func handlerCreateJob(c *fiber.Ctx) error {

	////////////////
	// Parse body //
	////////////////
	body := &JobsBody{}
	err := json.Unmarshal(c.Body(), body)
	if err != nil {
		zap.S().Warnf("Jobs POST Handler ERROR: %s", err.Error())

		c.Status(422)
		return c.SendString(`{"error": "could not parse body parameters"}`)
	}
	if body.EndBlockNumber < body.StartBlockNumber {
		c.Status(422)
		return c.SendString(`{"error": "end_block_number greater than start_block_number"}`)
	}

	////////////////
	// Create job //
	////////////////
	job := &models.Job{}
	job.StartBlockNumber = int64(body.StartBlockNumber)
	job.EndBlockNumber = int64(body.EndBlockNumber)
	job.CreatedTimestamp = time.Now().Unix()
	job.NumClaims = int64(math.Ceil(float64(body.EndBlockNumber-body.StartBlockNumber) / float64(config.Config.MaxClaimSize)))

	// Hash jobs
	// NOTE since timestamp will always be different, unique hash every request
	jobHash := sha256.New()
	jobHash.Write([]byte(fmt.Sprintf("%v", job)))
	job.Hash = fmt.Sprintf("%x", jobHash.Sum(nil))

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

	/////////////
	// Respond //
	/////////////
	res, _ := json.Marshal(job)
	return c.SendString(string(res))
}
