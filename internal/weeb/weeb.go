package weeb

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultLimit = 1 << 0
const defaultBufSize = 1

type Client interface {
	Chapters(ctx context.Context, u *url.URL) iter.Seq2[*url.URL, error]
	Images(ctx context.Context, u *url.URL) iter.Seq2[*url.URL, error]
	Image(ctx context.Context, u *url.URL) (io.ReadCloser, error)
}

type Handler struct {
	Client
}

func (h *Handler) Series(ctx context.Context, u *url.URL) io.ReadCloser {
	g, ctx := errgroup.WithContext(ctx)

	chapters := make(chan *url.URL) // defaultLimit
	g.Go(func() error {
		defer close(chapters)

		for url, err := range h.Client.Chapters(ctx, u) {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case chapters <- url:
			}
		}
		return nil
	})

	pr, pw := io.Pipe()
	g.Go(func() error {
		zw := zip.NewWriter(pw) // ~4kb
		defer zw.Close()

		var count int
		for u := range chapters {
			fh := &zip.FileHeader{
				Name:     strconv.Itoa(count) + ".zip",
				Method:   zip.Store,
				Modified: time.Now().UTC(),
			}
			w, err1 := zw.CreateHeader(fh)
			_, err2 := io.CopyBuffer(w, h.Chapter(ctx, u), nil) // reuseable buffer
			if err := cmp.Or(err1, err2); err != nil {
				return err
			}
			count++
		}
		return zw.Flush()
	})

	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}

func (h *Handler) Chapter(ctx context.Context, u *url.URL) io.ReadCloser {
	g, ctx := errgroup.WithContext(ctx)

	images := make(chan *url.URL, defaultBufSize)
	g.Go(func() error {
		defer close(images)

		for url, err := range h.Client.Images(ctx, u) {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case images <- url:
			}
		}
		return nil
	})

	bufs := make(chan *bytes.Buffer, defaultBufSize)
	g.Go(func() error {
		defer close(bufs)

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(1)
		for img := range images {
			g.Go(func() error {
				rc, err := h.Client.Image(ctx, img)
				if err != nil {
					return err
				}
				defer rc.Close()

				var buf bytes.Buffer
				// See https://destel.dev/blog/on-the-fly-content-type-detection-in-go
				if _, err = io.CopyN(&buf, rc, 512); err != nil {
					return err
				}
				switch ct := http.DetectContentType(buf.Bytes()); path.Dir(ct) { // get the top part
				case "image": // content-type can be spoofed
				default:
					return fmt.Errorf("unsupported image type: %q", ct)
				}
				_, err = io.Copy(&buf, rc)
				if err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case bufs <- &buf:
				}
				return nil
			})
		}
		return g.Wait()
	})

	pr, pw := io.Pipe()
	g.Go(func() error {
		zw := zip.NewWriter(pw) // ~4kb
		defer zw.Close()

		var count int
		for buf := range bufs {
			fh := &zip.FileHeader{
				Name:     strconv.Itoa(count) + ".jpeg",
				Method:   zip.Store,
				Modified: time.Now().UTC(),
			}
			w, err1 := zw.CreateHeader(fh)
			_, err2 := io.CopyBuffer(w, buf, nil) // reuseable buffer
			if err := cmp.Or(err1, err2); err != nil {
				return err
			}
			count++
		}
		return zw.Flush()
	})
	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}
