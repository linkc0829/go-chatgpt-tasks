package task

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/auth"
	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type service interface {
	Create(ctx context.Context, id Identity, in CreateInput) (*JobRun, error)
	List(ctx context.Context, id Identity, p shared.Pagination) ([]*JobRun, int64, error)
	Status(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error)
	Cancel(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error)
	RunsForJob(ctx context.Context, id Identity, jobID shared.JobID, p shared.Pagination) ([]*JobRun, int64, error)
	EventsForRun(ctx context.Context, id Identity, runID shared.JobRunID) ([]*RunEvent, error)
}

type Handler struct {
	svc      service
	resolver TenantResolver
}

func NewHandler(svc service, resolver TenantResolver) *Handler {
	return &Handler{svc: svc, resolver: resolver}
}

func (h *Handler) create(c *gin.Context) {
	id, ok := h.identity(c)
	if !ok {
		return
	}

	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	in, err := req.toInput()
	if err != nil {
		writeError(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	run, err := h.svc.Create(ctx, id, in)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, runToHTTPResponse(run))
}

func (h *Handler) list(c *gin.Context) {
	id, ok := h.identity(c)
	if !ok {
		return
	}
	p := shared.NewPagination(parseInt(c.Query("limit")), parseInt(c.Query("offset")))

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	runs, total, err := h.svc.List(ctx, id, p)
	if err != nil {
		writeError(c, err)
		return
	}
	out := make([]RunResponse, 0, len(runs))
	for _, run := range runs {
		out = append(out, runToHTTPResponse(run))
	}
	c.JSON(http.StatusOK, ListRunsResponse{Runs: out, Total: total, Limit: p.Limit, Offset: p.Offset})
}

func (h *Handler) status(c *gin.Context) {
	id, runID, ok := h.runID(c)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	run, err := h.svc.Status(ctx, id, runID)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, runToHTTPResponse(run))
}

func (h *Handler) cancel(c *gin.Context) {
	id, runID, ok := h.runID(c)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	run, err := h.svc.Cancel(ctx, id, runID)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, runToHTTPResponse(run))
}

func (h *Handler) runsForJob(c *gin.Context) {
	id, ok := h.identity(c)
	if !ok {
		return
	}
	jobID, err := shared.ParseJobID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	p := shared.NewPagination(parseInt(c.Query("limit")), parseInt(c.Query("offset")))

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	runs, total, err := h.svc.RunsForJob(ctx, id, jobID, p)
	if err != nil {
		writeError(c, err)
		return
	}
	out := make([]RunResponse, 0, len(runs))
	for _, run := range runs {
		out = append(out, runToHTTPResponse(run))
	}
	c.JSON(http.StatusOK, ListRunsResponse{Runs: out, Total: total, Limit: p.Limit, Offset: p.Offset})
}

func (h *Handler) eventsForRun(c *gin.Context) {
	id, runID, ok := h.runID(c)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	events, err := h.svc.EventsForRun(ctx, id, runID)
	if err != nil {
		writeError(c, err)
		return
	}
	out := make([]RunEventResponse, 0, len(events))
	for _, event := range events {
		out = append(out, eventToHTTPResponse(event))
	}
	c.JSON(http.StatusOK, ListEventsResponse{Events: out})
}

func (h *Handler) runID(c *gin.Context) (Identity, shared.JobRunID, bool) {
	id, ok := h.identity(c)
	if !ok {
		return Identity{}, shared.JobRunID{}, false
	}
	runID, err := shared.ParseJobRunID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return Identity{}, shared.JobRunID{}, false
	}
	return id, runID, true
}

func (h *Handler) identity(c *gin.Context) (Identity, bool) {
	sub := auth.UserIDFromContext(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthenticated"})
		return Identity{}, false
	}
	userID, err := shared.ParseUserID(sub)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid subject"})
		return Identity{}, false
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	tenantID, err := h.resolver.ResolveTenant(ctx, userID)
	if err != nil {
		writeError(c, err)
		return Identity{}, false
	}
	return Identity{TenantID: tenantID, UserID: userID}, true
}

func writeError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrJobNotFound), errors.Is(err, ErrJobRunNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, ErrInvalidDescription),
		errors.Is(err, ErrInvalidSchedule),
		errors.Is(err, ErrInvalidOwner):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
