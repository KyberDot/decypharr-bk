package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

const (
	defaultChunkSize  = 512 * 1024 // 512 KB per chunk
	defaultQueueDepth = 4          // Total buffered data ≈ 2 MB
)

type streamError struct {
	Err                   error
	StatusCode            int
	IsClientDisconnection bool
}

func (e *streamError) Error() string {
	return e.Err.Error()
}

func (e *streamError) Unwrap() error {
	return e.Err
}

func getDownloadByteRange(info *manager.FileInfo) *[2]int64 {
	return info.ByteRange()
}

func (h *Handler) StreamResponse(info *manager.FileInfo, w http.ResponseWriter, r *http.Request) error {
	start, end := h.getRange(info, r)

	resp, err := h.manager.Stream(r.Context(), info.Parent(), info.Name(), start, end)
	if err != nil {
		return &streamError{Err: err, StatusCode: http.StatusRequestedRangeNotSatisfiable}
	}
	defer func(body io.ReadCloser) {
		_ = body.Close()
	}(resp.Body)
	return h.handleSuccessfulResponse(r.Context(), w, resp, start, end)
}

func (h *Handler) handleSuccessfulResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response, start, end int64) error {
	statusCode := http.StatusOK
	if start > 0 || end > 0 {
		statusCode = http.StatusPartialContent
	}

	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		w.Header().Set("Content-Length", contentLength)
	}

	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" && statusCode == http.StatusPartialContent {
		w.Header().Set("Content-Range", contentRange)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	w.Header().Set("Accept-Ranges", "bytes")
	return h.streamWithRingBuffer(ctx, w, resp.Body, statusCode)
}

type bufferedChunk struct {
	buf []byte
	n   int
}

func (h *Handler) streamWithRingBuffer(ctx context.Context, w http.ResponseWriter, src io.Reader, statusCode int) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("response does not support flushing")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool := sync.Pool{
		New: func() any {
			return make([]byte, defaultChunkSize)
		},
	}

	chunkCh := make(chan bufferedChunk, defaultQueueDepth)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)

		var readErr error

	readLoop:
		for {
			if err := ctx.Err(); err != nil {
				readErr = err
				break readLoop
			}

			buf := pool.Get().([]byte)
			n, err := src.Read(buf)

			if n > 0 {
				select {
				case chunkCh <- bufferedChunk{buf: buf, n: n}:
				case <-ctx.Done():
					pool.Put(buf)
					readErr = ctx.Err()
					break readLoop
				}
			} else {
				pool.Put(buf)
			}

			if err != nil {
				if errors.Is(err, io.EOF) {
					readErr = nil
				} else {
					readErr = err
				}
				break readLoop
			}
		}

		errCh <- readErr
	}()

	w.WriteHeader(statusCode)

	var writeErr error

writeLoop:
	for {
		select {
		case <-ctx.Done():
			writeErr = &streamError{Err: ctx.Err(), StatusCode: 0, IsClientDisconnection: true}
			break writeLoop

		case chunk, ok := <-chunkCh:
			if !ok {
				break writeLoop
			}

			_, err := w.Write(chunk.buf[:chunk.n])
			pool.Put(chunk.buf)

			if err != nil {
				if isClientDisconnection(err) {
					writeErr = &streamError{Err: err, StatusCode: 0, IsClientDisconnection: true}
				} else {
					writeErr = &streamError{Err: err, StatusCode: http.StatusInternalServerError}
				}
				break writeLoop
			}

			flusher.Flush()
		}
	}

	cancel()
	readErr := <-errCh

	if writeErr != nil {
		return writeErr
	}

	if readErr != nil {
		if isClientDisconnection(readErr) {
			return &streamError{Err: readErr, StatusCode: 0, IsClientDisconnection: true}
		}
		return &streamError{Err: readErr, StatusCode: http.StatusInternalServerError}
	}

	return nil
}

func (h *Handler) getRange(info *manager.FileInfo, r *http.Request) (int64, int64) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		if byteRange := getDownloadByteRange(info); byteRange != nil {
			return byteRange[0], byteRange[1]
		}
		return 0, 0
	}

	ranges, err := parseRange(rangeHeader, info.Size())
	if err != nil || len(ranges) != 1 {
		return 0, 0
	}

	byteRange := getDownloadByteRange(info)
	start, end := ranges[0].start, ranges[0].end

	if byteRange != nil {
		start += byteRange[0]
		end += byteRange[0]
	}
	return start, end
}
