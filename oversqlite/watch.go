package oversqlite

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) startDownloaderLoop(ctx context.Context) {
	useWatch, err := c.shouldUseBundleChangeWatch(ctx)
	if err != nil {
		if isProtocolVersionMismatch(err) {
			c.logger.Error("automatic downloads stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
			return
		}
	}
	if useWatch {
		c.watchAwareDownloaderLoop(ctx)
		return
	}
	c.downloaderLoop(ctx)
}

func (c *Client) shouldUseBundleChangeWatch(ctx context.Context) (bool, error) {
	if c == nil || c.config == nil || c.config.BundleChangeWatchMode != BundleChangeWatchAuto {
		return false, nil
	}
	if err := c.beginWatchSetupSyncOperation(ctx); err != nil {
		return false, err
	}
	connected := c.ensureConnectedSessionLocked(ctx, "Start() bundle change watch") == nil
	c.writeMu.Unlock()
	if !connected {
		return false, nil
	}
	return c.serverSupportsBundleChangeWatch(ctx)
}

func (c *Client) beginWatchSetupSyncOperation(ctx context.Context) error {
	for {
		err := c.tryBeginSyncOperation()
		if err == nil {
			return nil
		}
		if !IsExpectedSyncContention(err) {
			return err
		}
		if err := sleepWatchInterval(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
}

func (c *Client) serverSupportsBundleChangeWatch(ctx context.Context) (bool, error) {
	result, err := c.doAuthenticatedBoundedRequestWithRetry(
		ctx, "watch_capabilities", http.MethodGet,
		strings.TrimRight(c.BaseURL, "/")+"/sync/capabilities", nil, "",
		snapshotCapabilitiesBodyLimit, snapshotControlBodyLimit, nil, nil,
	)
	if err != nil || result.statusCode != http.StatusOK {
		return false, nil
	}
	var caps oversync.CapabilitiesResponse
	if err := json.Unmarshal(result.body, &caps); err != nil {
		return false, nil
	}
	if err := requireSupportedProtocolVersion(caps.ProtocolVersion); err != nil {
		return false, err
	}
	return caps.Features != nil && caps.Features["bundle_change_watch"], nil
}

func (c *Client) normalizedWatchFallbackInterval() time.Duration {
	if c != nil && c.config != nil {
		if c.config.WatchFallbackInterval > 0 {
			return c.config.WatchFallbackInterval
		}
		if c.config.BackoffMax > 0 {
			return c.config.BackoffMax
		}
	}
	return 60 * time.Second
}

func (c *Client) watchAwareDownloaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	if backoff <= 0 {
		backoff = time.Second
	}
	fallbackInterval := c.normalizedWatchFallbackInterval()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomicDownloadPaused(c) {
			if err := sleepWatchInterval(ctx, backoff); err != nil {
				return
			}
			continue
		}

		afterBundleSeq, err := c.LastBundleSeqSeen(ctx)
		if err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				if err := sleepWatchInterval(ctx, backoff); err != nil {
					return
				}
				continue
			}
			if err := sleepWatchInterval(ctx, backoff); err != nil {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		if err := c.watchRemoteChanges(ctx, afterBundleSeq, fallbackInterval); err != nil {
			if ctx.Err() != nil {
				return
			}
			if isProtocolVersionMismatch(err) {
				c.logger.Error("bundle change watch loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
				return
			}
			if isBundleChangeWatchForbidden(err) {
				c.logger.Warn("bundle change watch disabled for client", "error", err)
				c.downloaderLoop(ctx)
				return
			}
			c.logger.Warn("bundle change watch stream stopped", "error_type", fmt.Sprintf("%T", err))
			backoffErr := err
			var streamPullErr *bundleChangeWatchPullError
			if !errors.As(err, &streamPullErr) {
				if _, pullErr := c.PullToStable(ctx); pullErr != nil {
					if ctx.Err() != nil {
						return
					}
					if isProtocolVersionMismatch(pullErr) {
						c.logger.Error("bundle change watch loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", pullErr))
						return
					}
					backoffErr = pullErr
					if IsLifecyclePreconditionError(pullErr) {
						c.logger.Warn("download loop blocked by lifecycle state", "error", pullErr)
					} else {
						c.logger.Warn("bundle change watch fallback pull failed", "error", pullErr)
					}
				}
			}
			if err := sleepWatchInterval(ctx, backoff); err != nil {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(backoffErr, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}
		backoff = c.config.BackoffMin
	}
}

func atomicDownloadPaused(c *Client) bool {
	return c != nil && atomic.LoadInt32(&c.downloadPaused) == 1
}

type bundleChangeWatchPullError struct {
	err error
}

func (e *bundleChangeWatchPullError) Error() string {
	if e == nil || e.err == nil {
		return "bundle change watch pull failed"
	}
	return e.err.Error()
}

func (e *bundleChangeWatchPullError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

const (
	bundleChangeWatchMaxLineBytes  = 1 << 20
	bundleChangeWatchMaxEventBytes = 1 << 20
)

var (
	errBundleChangeWatchForbidden     = errors.New("bundle change watch forbidden")
	errBundleChangeWatchLineTooLarge  = errors.New("bundle change watch line exceeds byte limit")
	errBundleChangeWatchEventTooLarge = errors.New("bundle change watch event exceeds byte limit")
)

func isBundleChangeWatchForbidden(err error) bool {
	return errors.Is(err, errBundleChangeWatchForbidden)
}

func sleepWatchInterval(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) watchRemoteChanges(ctx context.Context, afterBundleSeq int64, fallbackInterval time.Duration) error {
	resp, err := c.openBundleChangeWatch(ctx, afterBundleSeq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	eventCh := make(chan oversync.BundleChangeEvent, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- parseBundleChangeWatchStream(ctx, resp.Body, eventCh)
		close(eventCh)
	}()

	fallback := time.NewTicker(fallbackInterval)
	defer fallback.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-eventCh:
			if !ok {
				if err := <-errCh; err != nil {
					return err
				}
				return io.EOF
			}
			if event.BundleSeq <= 0 {
				return fmt.Errorf("bundle watch event missing bundle_seq")
			}
			if _, err := c.PullToStable(ctx); err != nil {
				return &bundleChangeWatchPullError{err: err}
			}
		case <-fallback.C:
			if _, err := c.PullToStable(ctx); err != nil {
				return &bundleChangeWatchPullError{err: err}
			}
		}
	}
}

func (c *Client) openBundleChangeWatch(ctx context.Context, afterBundleSeq int64) (*http.Response, error) {
	if afterBundleSeq < 0 {
		return nil, fmt.Errorf("after_bundle_seq must be >= 0")
	}
	sourceID, err := c.refreshCurrentSourceIDForWatch(ctx)
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/sync/watch?after_bundle_seq=" + strconv.FormatInt(afterBundleSeq, 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, err
	}
	if err := applyAuthenticatedSyncHeadersWithSourceID(req, token, sourceID); err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, readErr := readDecodedBodyBounded(ctx, "bundle_change_watch", resp, snapshotControlBodyLimit)
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusForbidden {
			if errorResp, ok := decodeServerErrorResponse(body); ok && errorResp.Error == "bundle_change_watch_forbidden" {
				return nil, errBundleChangeWatchForbidden
			}
		}
		return nil, &retryHTTPError{
			Operation:  "bundle_change_watch",
			StatusCode: resp.StatusCode,
		}
	}
	return resp, nil
}

func (c *Client) refreshCurrentSourceIDForWatch(ctx context.Context) (string, error) {
	if c == nil {
		return "", nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		if validateErr := validateOptionalSourceID(c.sourceID); validateErr != nil {
			return "", validateErr
		}
		return c.sourceID, nil
	}
	defer c.writeMu.Unlock()
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return "", err
	}
	if attachment.CurrentSourceID != "" {
		c.sourceID = attachment.CurrentSourceID
	}
	if err := validateOptionalSourceID(c.sourceID); err != nil {
		return "", err
	}
	return c.sourceID, nil
}

func parseBundleChangeWatchStream(ctx context.Context, r io.Reader, events chan<- oversync.BundleChangeEvent) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024), bundleChangeWatchMaxLineBytes+2)

	var eventName string
	var data strings.Builder
	eventBytes := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > bundleChangeWatchMaxLineBytes {
			return errBundleChangeWatchLineTooLarge
		}
		if line == "" {
			if eventName == "bundle" {
				var event oversync.BundleChangeEvent
				if err := json.Unmarshal([]byte(data.String()), &event); err != nil {
					return fmt.Errorf("decode bundle watch event: %w", err)
				}
				if err := validateOptionalSourceID(event.SourceID); err != nil {
					return fmt.Errorf("bundle watch event source_id is invalid: %w", err)
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			eventName = ""
			data.Reset()
			eventBytes = 0
			continue
		}
		lineBytes := len(line) + 1
		if lineBytes > bundleChangeWatchMaxEventBytes-eventBytes {
			return errBundleChangeWatchEventTooLarge
		}
		eventBytes += lineBytes
		if strings.HasPrefix(line, ":") {
			continue
		}
		if value, ok := strings.CutPrefix(line, "event:"); ok {
			eventName = strings.TrimPrefix(value, " ")
			continue
		}
		if value, ok := strings.CutPrefix(line, "data:"); ok {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(value, " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}
