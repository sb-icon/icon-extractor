package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	swagger "github.com/arsmn/fiber-swagger/v2"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"go.uber.org/zap"

	_ "github.com/sudoblockio/icon-extractor/api/docs" // import for swagger docs
	"github.com/sudoblockio/icon-extractor/api/routes"
	"github.com/sudoblockio/icon-extractor/config"
)

// @title Go api template docs
// @version 2.0
// @description This is a sample server server.
func Start() {

	app := fiber.New()

	// Logging middleware
	app.Use(func(c *fiber.Ctx) error {
		zap.S().Info(c.Method(), " ", c.Path())

		// Go to next middleware:
		return c.Next()
	})

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

		// Loop until success
		for {

			body := strings.NewReader(fmt.Sprintf(`{
  					"start_block_number": %d,
  					"end_block_number": %d
				}`,
				config.Config.InsertExtractorJobStartBlockNumber,
				config.Config.InsertExtractorJobEndBlockNumber,
			))
			req, err := http.NewRequest("POST", "http://localhost:" + config.Config.APIPort + config.Config.APIPrefix + "/create-job", body)
			if err != nil {
				zap.S().Warn(err)
				time.Sleep(1 * time.Second)
				continue
			}
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Content-Type", "*/*")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				zap.S().Warn(err)
				time.Sleep(1 * time.Second)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == 200 {
				// Success
				break
			}

			// Fail
			zap.S().Warn("Could not insert extractor job, StatusCode=", resp.StatusCode)
			time.Sleep(1 * time.Second)
			continue
		}
	}
}
