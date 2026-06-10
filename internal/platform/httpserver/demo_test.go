package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestRegisterDemo(t *testing.T) {
	engine := New(zap.NewNop())
	RegisterDemo(engine)

	for _, path := range []string{"/demo/", "/demo/app.css", "/demo/app.js"} {
		t.Run(strings.TrimPrefix(path, "/demo/"), func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)

			engine.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, http.StatusOK)
			}
			if recorder.Body.Len() == 0 {
				t.Fatalf("GET %s returned an empty body", path)
			}
		})
	}
}
