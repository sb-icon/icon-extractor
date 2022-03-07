package api

import (
	swagger "github.com/arsmn/fiber-swagger/v2"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"go.uber.org/zap"

	_ "github.com/sudoblockio/icon-go-etl/api/docs" // import for swagger docs
	"github.com/sudoblockio/icon-go-etl/api/routes"
	"github.com/sudoblockio/icon-go-etl/config"
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
}
