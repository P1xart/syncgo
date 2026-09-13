package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

//go:generate go tool mockgen -source=batcher.go -destination=./mocks/bulk_sender_mock.go -package=mocks
type BulkSender interface {
	Bulk(ctx context.Context, data bulk_transformer.DataPayload) error
}

// FlushMonitoring reports flush retry activity. Both methods are optional:
// a nil FlushMonitoring passed to NewBatcher disables reporting.
type FlushMonitoring interface {
	IncFlushRetry()
	IncFlushFailure()
}

// Config holds retry settings for Batcher.Flush.
type Config struct {
	BufferSize   int
	FlushTimeout time.Duration
	// MaxRetries is how many extra attempts are made after a failed flush
	// before giving up. 0 means no retries.
	MaxRetries int
	// RetryTimeout is the delay between flush retry attempts.
	RetryTimeout time.Duration
}

type Batcher struct {
	mu sync.Mutex

	buffer       []bulk_transformer.Data
	bufferSize   int
	flushTimeout time.Duration
	maxRetries   int
	retryTimeout time.Duration

	// lastCommitted is the index of the last committed item in buffer.
	// -1 means nothing committed yet.
	lastCommitted int

	// lastFlushTime is updated after every successful flush; used by StartFlushLoop
	// to avoid firing a timeout flush too soon after a size-triggered flush.
	lastFlushTime time.Time

	sender     BulkSender
	monitoring FlushMonitoring
	ctx        context.Context
}

func NewBatcher(ctx context.Context, cfg Config, monitoring FlushMonitoring, sender BulkSender) *Batcher {
	batcher := &Batcher{
		ctx:           ctx,
		buffer:        make([]bulk_transformer.Data, 0, cfg.BufferSize),
		bufferSize:    cfg.BufferSize,
		flushTimeout:  cfg.FlushTimeout,
		maxRetries:    cfg.MaxRetries,
		retryTimeout:  cfg.RetryTimeout,
		lastCommitted: -1,
		sender:        sender,
		monitoring:    monitoring,
		lastFlushTime: time.Now(),
	}

	if cfg.FlushTimeout > 0 {
		go batcher.startFlushLoop()
	}

	return batcher
}

func (b *Batcher) startFlushLoop() {
	ticker := time.NewTicker(b.flushTimeout)
	defer ticker.Stop()

	for b.ctx.Err() == nil {
		select {
		case <-b.ctx.Done():
			slog.Info("batcher context done")
			return
		case <-ticker.C:
			b.mu.Lock()
			idle := time.Since(b.lastFlushTime)
			shouldFlush := b.flushTimeout > 0 && idle >= b.flushTimeout
			if shouldFlush {
				_ = b.flushLocked()
			}
			b.mu.Unlock()
		}
	}
}

// Add appends a new item into the batch.
// If the buffer is full, committed items are flushed first.
func (b *Batcher) Add(item bulk_transformer.Data) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) >= b.bufferSize {
		_ = b.flushLocked()
	}

	b.buffer = append(b.buffer, item)
}

// CommitN calls Commit method n times
// Commit order must match Add order.
func (b *Batcher) CommitN(n int) {
	for i := 0; i < n; i++ {
		b.Commit()
	}
}

// Commit marks the next item in order as committed.
// Commit order must match Add order.
func (b *Batcher) Commit() {
	b.mu.Lock()
	defer b.mu.Unlock()

	nextCommit := b.lastCommitted + 1
	if nextCommit >= len(b.buffer) {
		return
	}

	b.lastCommitted = nextCommit
}

// RollbackN calls Rollback n times.
func (b *Batcher) RollbackN(n int) {
	for i := 0; i < n; i++ {
		b.Rollback()
	}
}

// Rollback removes the latest uncommitted item.
func (b *Batcher) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return
	}

	lastIndex := len(b.buffer) - 1

	// Cannot rollback committed data
	if lastIndex <= b.lastCommitted {
		return
	}

	b.buffer = b.buffer[:lastIndex]
}

// Flush sends only committed items.
// It returns the last error if the flush and all its retries failed.
func (b *Batcher) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.flushLocked()
}

// flushLocked flushes committed prefix, retrying on failure up to maxRetries
// times with a retryTimeout delay between attempts.
// Caller must hold mutex.
func (b *Batcher) flushLocked() error {
	if b.lastCommitted < 0 {
		return nil
	}

	flushLen := b.lastCommitted + 1
	payload := bulk_transformer.DataPayload(b.buffer[:flushLen])

	var err error
	for attempt := 1; attempt <= b.maxRetries+1; attempt++ {
		err = b.sender.Bulk(b.ctx, payload)
		if err == nil {
			break
		}

		slog.Error("batcher: bulk send failed",
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", b.maxRetries+1),
			slog.Int("last_committed", b.lastCommitted),
			slog.Int("flush_len", flushLen),
			slog.String("error", err.Error()),
		)

		if attempt > b.maxRetries {
			break
		}

		if b.monitoring != nil {
			b.monitoring.IncFlushRetry()
		}

		select {
		case <-b.ctx.Done():
			err = b.ctx.Err()
			if b.monitoring != nil {
				b.monitoring.IncFlushFailure()
			}
			return err
		case <-time.After(b.retryTimeout):
		}
	}

	if err != nil {
		slog.Error("batcher: flush failed, giving up after retries",
			slog.Int("attempts", b.maxRetries+1),
			slog.Int("flush_len", flushLen),
			slog.String("error", err.Error()),
		)
		if b.monitoring != nil {
			b.monitoring.IncFlushFailure()
		}
		return err
	}

	// Keep only uncommitted tail
	b.buffer = b.buffer[flushLen:]

	// Reset commit pointer relative to new buffer
	b.lastCommitted = -1
	b.lastFlushTime = time.Now()

	return nil
}
