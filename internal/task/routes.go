package task

import (
	"github.com/gin-gonic/gin"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/auth"
)

func RegisterRoutes(rg *gin.RouterGroup, h *Handler, authMW *auth.Manager) {
	jobs := rg.Group("/jobs")
	jobs.Use(auth.Middleware(authMW))
	{
		jobs.POST("", h.create)
		jobs.GET("", h.list)
		jobs.GET("/:id", h.status)
		jobs.POST("/:id/cancel", h.cancel)
	}
}
