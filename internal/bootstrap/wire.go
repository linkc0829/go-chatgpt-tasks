package bootstrap

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/linkc0829/go-backend-template/internal/platform/auth"
	"github.com/linkc0829/go-backend-template/internal/task"
	"github.com/linkc0829/go-backend-template/internal/user"
)

// wireFeatures registers every feature slice with the gin engine.
//
// THIS IS THE ONLY PLACE in the codebase that imports more than one feature
// package. That is by design — features stay decoupled, and cross-feature
// dependencies become explicit here.
func wireFeatures(
	engine *gin.Engine,
	pool *pgxpool.Pool,
	rdb *redis.Client,
	authMgr *auth.Manager,
	lg *zap.Logger,
) []task.Runner {
	api := engine.Group("/api/v1")

	// ------------------------------------------------------------------
	// User feature
	// ------------------------------------------------------------------
	userRepo := user.NewPostgresRepo(pool)
	userHasher := user.NewBcryptHasher(0) // 0 = bcrypt.DefaultCost
	userSvc := user.NewService(userRepo, userHasher, authMgr)
	userHandler := user.NewHandler(userSvc)
	user.RegisterRoutes(api, userHandler, authMgr)

	taskRepo := task.NewPostgresRepo(pool)
	taskQueue := task.NewRedisQueue(rdb)
	watcher := task.NewWatcher(taskRepo, taskQueue, 5*time.Second, lg)
	exec := task.NewStubExecutor(lg)

	const workerCount = 3
	runners := []task.Runner{watcher}
	for i := 0; i < workerCount; i++ {
		runners = append(runners, task.NewWorker(fmt.Sprintf("worker-%d", i), taskRepo, taskQueue, exec, lg))
	}
	runners = append(runners, task.NewRecurringWatcher(taskRepo, 10*time.Second, lg))
	return runners
}
