package http

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/adoublef/weeb/internal/encoding/html"
)

type Client struct {
	*http.Client
}

func (c *Client) Chapters(ctx context.Context, u *url.URL) iter.Seq2[*url.URL, error] {
	return func(yield func(*url.URL, error) bool) {
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, u.JoinPath("full-chapter-list").String(), nil)
		res, err2 := c.Do(req)
		if err := cmp.Or(err1, err2); err != nil {
			yield(nil, err)
			return
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			yield(nil, fmt.Errorf("failed to query %q: %v", req.URL, StatusCode(c)))
			return
		}
		// limit body?
		for url, err := range html.Anchors(res.Body) {
			if err != nil {
				yield(nil, err)
				return
			}
			path := strings.TrimPrefix(url.Path, "/")
			first, rest, more := strings.Cut(path, "/")
			if !more || first == "" || rest == "" || strings.Contains(rest, "/") {
				yield(nil, fmt.Errorf("invalid chapter url"))
				return
			}
			if !yield(url, nil) {
				return
			}
		}
	}
}

func (c *Client) Images(ctx context.Context, u *url.URL) iter.Seq2[*url.URL, error] {
	return func(yield func(*url.URL, error) bool) {
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, u.JoinPath("images").String(), nil)
		res, err2 := c.Do(req)
		if err := cmp.Or(err1, err2); err != nil {
			yield(nil, err)
			return
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			yield(nil, fmt.Errorf("failed to query %q: %v", req.URL, StatusCode(c)))
			return
		}
		// limit body?
		for url, err := range html.Images(res.Body) {
			if err != nil {
				yield(nil, err)
				return
			}
			name := path.Base(url.Path)
			n, err := fmt.Sscanf(name, "%d-%d.%s", new(uint), new(uint), new(string))
			if err != nil || n != 3 {
				yield(nil, fmt.Errorf("name %q invalid", name)) // bad verb '%d' for string
				return
			}
			if !yield(url, nil) {
				return
			}
		}
	}
}

func (c *Client) Image(ctx context.Context, u *url.URL) (io.ReadCloser, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	res, err2 := c.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return nil, err
	}
	if c := res.StatusCode; c != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("failed to query %q: %v", req.URL, StatusCode(c))
	}
	// limit this with the content-type
	return res.Body, nil
}
