package oversqlite

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	snapshotCapabilitiesBodyLimit int64 = 4 << 20
	snapshotControlBodyLimit      int64 = 64 << 10
	snapshotBodyEnvelopeBytes     int64 = 64 << 10
	snapshotRetirementTimeout           = 5 * time.Second
)

// SnapshotResponseBodyTooLargeError reports that a decoded response exceeded
// the operation-specific memory boundary.
type SnapshotResponseBodyTooLargeError struct {
	Operation string
	Limit     int64
}

func (e *SnapshotResponseBodyTooLargeError) Error() string {
	return fmt.Sprintf("%s decoded response body exceeds %d bytes", e.Operation, e.Limit)
}

// SnapshotUnsupportedContentEncodingError reports a response encoding that
// the bounded reader cannot safely decode.
type SnapshotUnsupportedContentEncodingError struct {
	Operation string
	Encoding  string
}

func (e *SnapshotUnsupportedContentEncodingError) Error() string {
	return fmt.Sprintf("%s response uses unsupported content encoding", e.Operation)
}

type boundedHTTPFailureStage string

const (
	boundedHTTPFailureTransport boundedHTTPFailureStage = "transport"
	boundedHTTPFailureDecode    boundedHTTPFailureStage = "response_decode"
	boundedHTTPFailureRead      boundedHTTPFailureStage = "response_read"
	boundedHTTPFailureClose     boundedHTTPFailureStage = "response_close"
)

// boundedHTTPFailureError preserves only local classification state. It must
// never retain or unwrap the transport/body error supplied by the peer.
type boundedHTTPFailureError struct {
	operation string
	stage     boundedHTTPFailureStage
	retryable bool
}

func (e *boundedHTTPFailureError) Error() string {
	if e == nil {
		return "oversqlite HTTP operation failed"
	}
	return fmt.Sprintf("%s HTTP %s failed", e.operation, e.stage)
}

func newBoundedHTTPFailureError(ctx context.Context, operation string, stage boundedHTTPFailureStage, err error) error {
	return &boundedHTTPFailureError{
		operation: operation,
		stage:     stage,
		retryable: isRetryableOperationError(ctx, err),
	}
}

type boundedResponsePayload struct {
	body             []byte
	statusCode       int
	decodedBodyBytes int64
	retryAfter       string
}

type snapshotCapacityHTTPError struct {
	errorCode  string
	retryAfter time.Duration
}

func (e *snapshotCapacityHTTPError) Error() string {
	return "snapshot admission capacity is temporarily unavailable"
}

type exactlyOnceReadCloser struct {
	body     io.ReadCloser
	once     sync.Once
	closeErr error
}

func (c *exactlyOnceReadCloser) Read(p []byte) (int, error) {
	return c.body.Read(p)
}

func (c *exactlyOnceReadCloser) Close() error {
	c.once.Do(func() {
		c.closeErr = c.body.Close()
	})
	return c.closeErr
}

func checkedAddInt64(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("integer overflow adding %d and %d", a, b)
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("integer overflow adding %d and %d", a, b)
	}
	return a + b, nil
}

func checkedSnapshotChunkBodyLimit(maxBytes int64, maxRows int) (int64, error) {
	if maxBytes <= 0 || maxRows <= 0 {
		return 0, fmt.Errorf("snapshot chunk body budget requires positive row and byte limits")
	}
	limit, err := checkedAddInt64(maxBytes, int64(maxRows))
	if err != nil {
		return 0, fmt.Errorf("snapshot chunk body budget overflow: %w", err)
	}
	limit, err = checkedAddInt64(limit, snapshotBodyEnvelopeBytes)
	if err != nil {
		return 0, fmt.Errorf("snapshot chunk body budget overflow: %w", err)
	}
	return limit, nil
}

func readDecodedBodyBounded(ctx context.Context, operation string, resp *http.Response, limit int64) ([]byte, error) {
	if limit < 0 || limit == math.MaxInt64 {
		return nil, fmt.Errorf("%s response body limit is invalid", operation)
	}
	if resp == nil || resp.Body == nil {
		return nil, fmt.Errorf("%s response body is missing", operation)
	}
	if _, ok := resp.Body.(*exactlyOnceReadCloser); !ok {
		resp.Body = &exactlyOnceReadCloser{body: resp.Body}
	}

	stopClose := context.AfterFunc(ctx, func() { _ = resp.Body.Close() })
	defer stopClose()

	var reader io.Reader = resp.Body
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
	case "gzip":
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureDecode, err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	default:
		return nil, &SnapshotUnsupportedContentEncodingError{Operation: operation}
	}

	readLimit := limit + 1
	body, err := io.ReadAll(io.LimitReader(reader, readLimit))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureRead, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if int64(len(body)) > limit {
		return nil, &SnapshotResponseBodyTooLargeError{Operation: operation, Limit: limit}
	}
	return body, nil
}

func (c *Client) doAuthenticatedBoundedRequest(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	var bodyReader io.Reader
	if len(requestBody) > 0 {
		bodyReader = strings.NewReader(string(requestBody))
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return boundedResponsePayload{}, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return boundedResponsePayload{}, err
	}
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	// Setting this explicitly disables net/http's transparent unbounded gzip
	// decoding. The bounded reader performs decoding below.
	req.Header.Set("Accept-Encoding", "gzip")
	if err := c.applyAuthenticatedSyncHeaders(req, token); err != nil {
		return boundedResponsePayload{}, err
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return boundedResponsePayload{}, ctxErr
		}
		return boundedResponsePayload{}, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureTransport, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()

	limit := errorLimit
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		limit = successLimit
	}
	body, readErr := readDecodedBodyBounded(ctx, operation, resp, limit)
	if readErr != nil {
		return boundedResponsePayload{}, readErr
	}
	if onDecodedBodyRead != nil {
		onDecodedBodyRead(int64(len(body)))
	}
	closeErr := resp.Body.Close()
	closed = true
	if closeErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return boundedResponsePayload{}, ctxErr
		}
		return boundedResponsePayload{}, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureClose, closeErr)
	}
	return boundedResponsePayload{
		body:             body,
		statusCode:       resp.StatusCode,
		decodedBodyBytes: int64(len(body)),
		retryAfter:       resp.Header.Get("Retry-After"),
	}, nil
}

func (c *Client) doAuthenticatedBoundedRequestWithRetry(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	transientErrorCodes map[string]struct{},
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	capacityPolicy := c.normalizedSnapshotCapacityRetryPolicy()
	var capacityStarted time.Time
	for {
		result, err := withRetryValue(ctx, c.normalizedRetryPolicy(), operation, func() (boundedResponsePayload, error) {
			result, err := c.doAuthenticatedBoundedRequest(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, onDecodedBodyRead)
			if err != nil {
				return boundedResponsePayload{}, err
			}
			retryable := result.statusCode == http.StatusBadGateway || result.statusCode == http.StatusServiceUnavailable || result.statusCode == http.StatusGatewayTimeout
			if result.statusCode == http.StatusTooManyRequests {
				if transientErrorCodes == nil {
					retryable = true
				} else if errorResp, ok := decodeServerErrorResponse(result.body); ok {
					if _, capacity := transientErrorCodes[errorResp.Error]; capacity {
						return boundedResponsePayload{}, &snapshotCapacityHTTPError{
							errorCode:  errorResp.Error,
							retryAfter: parseRetryAfterDeltaSeconds(result.retryAfter),
						}
					}
				}
			}
			if retryable {
				return boundedResponsePayload{}, &retryHTTPError{
					Operation: operation, StatusCode: result.statusCode,
				}
			}
			return result, nil
		})
		if err == nil {
			return result, nil
		}
		var capacityErr *snapshotCapacityHTTPError
		if !errors.As(err, &capacityErr) {
			return boundedResponsePayload{}, err
		}
		atomic.AddInt64(&c.snapshotStats.capacityResponses, 1)
		if capacityStarted.IsZero() {
			capacityStarted = time.Now()
		}
		waited := time.Since(capacityStarted)
		if !capacityPolicy.enabled {
			return boundedResponsePayload{}, &SnapshotCapacityRetryExhaustedError{
				Operation: operation, ErrorCode: capacityErr.errorCode, Waited: waited,
			}
		}
		delay := capacityErr.retryAfter
		if delay <= 0 {
			delay = capacityPolicy.fallbackDelay
		}
		delay = positiveJitterDuration(delay, capacityPolicy.jitterFraction)
		if delay <= 0 || waited >= capacityPolicy.maxWait || delay > capacityPolicy.maxWait-waited {
			return boundedResponsePayload{}, &SnapshotCapacityRetryExhaustedError{
				Operation: operation, ErrorCode: capacityErr.errorCode, Waited: waited,
			}
		}
		atomic.AddInt64(&c.snapshotStats.capacityRetries, 1)
		waitStarted := time.Now()
		if err := waitRetryBackoff(ctx, delay); err != nil {
			return boundedResponsePayload{}, err
		}
		atomic.AddInt64(&c.snapshotStats.capacityWaitNanos, int64(time.Since(waitStarted)))
	}
}

func parseRetryAfterDeltaSeconds(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func positiveJitterDuration(base time.Duration, fraction float64) time.Duration {
	if base <= 0 || fraction <= 0 {
		return base
	}
	span := float64(base) * min(fraction, 1)
	return base + time.Duration(math.Round(rand.Float64()*span))
}
