package routes

import (
	fiber "github.com/gofiber/fiber/v2"

	"github.com/geometry-labs/icon-go-etl/config"
)

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
func handlerCreateJob(c *fiber.Ctx) error {

	return c.SendString("asdasdasd")
}
