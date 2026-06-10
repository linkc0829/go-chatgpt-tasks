package httpserver

import (
	"embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed demo/*
var demoFiles embed.FS

// RegisterDemo serves the dependency-free browser demo from the API process.
func RegisterDemo(engine *gin.Engine) {
	engine.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusTemporaryRedirect, "/demo/")
	})
	engine.GET("/demo", func(c *gin.Context) {
		c.Redirect(http.StatusTemporaryRedirect, "/demo/")
	})
	engine.GET("/demo/", demoAsset("demo/index.html", "text/html; charset=utf-8"))
	engine.GET("/demo/app.css", demoAsset("demo/app.css", "text/css; charset=utf-8"))
	engine.GET("/demo/app.js", demoAsset("demo/app.js", "text/javascript; charset=utf-8"))
}

func demoAsset(name, contentType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		content, err := demoFiles.ReadFile(name)
		if err != nil {
			c.Status(http.StatusNotFound)
			return
		}
		c.Data(http.StatusOK, contentType, content)
	}
}
