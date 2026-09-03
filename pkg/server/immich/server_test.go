// Smoke tests for the store-independent endpoints. These do not need a
// populated Perkeep index: they confirm the generated router reaches our
// handlers and that responses encode. Store-backed methods are TODO[STUB] and
// are not exercised here.
package immich

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func newTestRouter() http.Handler {
	// A nil store is fine: these endpoints never touch the store.
	return HandlerFromMux(&Server{}, chi.NewRouter())
}

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestPingViaRouter(t *testing.T) {
	rec := get(t, newTestRouter(), http.MethodGet, "/server/ping")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"pong"`) {
		t.Errorf("body %s missing pong", rec.Body.String())
	}
}

func TestVersionViaRouter(t *testing.T) {
	rec := get(t, newTestRouter(), http.MethodGet, "/server/version")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"major":0`) {
		t.Errorf("unexpected version body: %s", rec.Body.String())
	}
}

func TestMyUserViaRouter(t *testing.T) {
	rec := get(t, newTestRouter(), http.MethodGet, "/users/me")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"perkeep"`) {
		t.Errorf("body %s missing owner name", rec.Body.String())
	}
}

func TestUnimplementedFallsThrough(t *testing.T) {
	// A write endpoint we do not serve must answer 501 via the embedded
	// Unimplemented, not panic.
	rec := get(t, newTestRouter(), http.MethodPost, "/albums")
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("POST /albums status=%d want 501", rec.Code)
	}
}
