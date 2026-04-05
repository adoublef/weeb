package http

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/trace"

	"github.com/adoublef/weeb/internal/weeb"
)

func Handler(h *weeb.Handler) http.Handler {
	return handleZip(h)
}

func handleZip(h *weeb.Handler) HandlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (*url.URL, error) {
		// allow deflate
		return url.Parse(r.URL.Query().Get("series_url"))
	}
	return func(w http.ResponseWriter, r *http.Request) error {
		ctx, task := trace.NewTask(r.Context(), "handleFunc")
		defer task.End()

		u, err := parse(w, r)
		if err != nil {
			return fmt.Errorf("invalid request: %v: %w", err, StatusBadRequest)
		}

		s := h.Series(ctx, u)
		defer s.Close()

		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Disposition", "attachment; filename=\"cbz.zip\"")

		_, err = io.Copy(w, s)
		return err
	}
}

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

func (h HandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h(w, r)
	if err == nil {
		return
	}
	if h, ok := err.(http.Handler); ok {
		h.ServeHTTP(w, r)
		return
	}
}

var StatusBadRequest = StatusCode(http.StatusBadRequest)

type StatusCode int

func (e StatusCode) Error() string { return http.StatusText(int(e)) }

func (e StatusCode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(e))
}
