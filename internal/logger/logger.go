package logger

import (
	"context"
	"encoding/json"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

// TrafficLogger ships traffic log records to Kafka.
// When Kafka is disabled, it falls back to an in-memory ring buffer
// that the admin API can serve directly.
type TrafficLogger struct {
	writer   *kafka.Writer
	fallback *RingBuffer
	enabled  bool
}

// NewTrafficLogger creates a logger. If enabled=false, all writes go to
// the in-memory fallback ring buffer.
func NewTrafficLogger(brokers []string, topic string, enabled bool) *TrafficLogger {
	tl := &TrafficLogger{
		fallback: NewRingBuffer(10_000),
		enabled:  enabled,
	}
	if enabled {
		tl.writer = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchSize:              100,
			BatchTimeout:           10 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			Async:                  true, // fire-and-forget — don't block the hot path
		}
	}
	return tl
}

// Log enqueues a traffic log record. This is designed to be called from
// the hot request path and must never block the caller.
func (tl *TrafficLogger) Log(entry *models.TrafficLog) {
	// Always write to ring buffer for the admin live-tail endpoint
	tl.fallback.Push(entry)

	if !tl.enabled || tl.writer == nil {
		return
	}

	data, err := json.Marshal(entry)
	if err != nil {
		log.Error().Err(err).Str("log_id", entry.ID).Msg("failed to marshal traffic log")
		return
	}

	msg := kafka.Message{
		Key:   []byte(entry.RouteID),
		Value: data,
		Time:  entry.Timestamp,
	}

	// Non-blocking write via async Kafka writer
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := tl.writer.WriteMessages(ctx, msg); err != nil {
		log.Warn().Err(err).Str("route_id", entry.RouteID).Msg("kafka write failed, log kept in ring buffer only")
	}
}

// Recent returns up to n recent log entries from the in-memory buffer.
func (tl *TrafficLogger) Recent(n int) []*models.TrafficLog {
	return tl.fallback.Tail(n)
}

// Close flushes the Kafka writer.
func (tl *TrafficLogger) Close() error {
	if tl.writer != nil {
		return tl.writer.Close()
	}
	return nil
}

// ── Ring Buffer ────────────────────────────────────────────────────────────

// RingBuffer is a lock-free-ish circular log store for in-process log tailing.
type RingBuffer struct {
	buf  []*models.TrafficLog
	size int
	head int
	count int
	mu   chan struct{} // 1-element channel as mutex
}

// NewRingBuffer allocates a ring buffer with the given capacity.
func NewRingBuffer(size int) *RingBuffer {
	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	return &RingBuffer{buf: make([]*models.TrafficLog, size), size: size, mu: mu}
}

// Push adds an entry. Oldest entry is overwritten when full.
func (r *RingBuffer) Push(entry *models.TrafficLog) {
	<-r.mu
	defer func() { r.mu <- struct{}{} }()
	r.buf[r.head] = entry
	r.head = (r.head + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

// Tail returns the n most recent entries in reverse-chronological order.
func (r *RingBuffer) Tail(n int) []*models.TrafficLog {
	<-r.mu
	defer func() { r.mu <- struct{}{} }()

	if n > r.count {
		n = r.count
	}
	out := make([]*models.TrafficLog, n)
	idx := (r.head - 1 + r.size) % r.size
	for i := 0; i < n; i++ {
		out[i] = r.buf[idx]
		idx = (idx - 1 + r.size) % r.size
	}
	return out
}

// ── Structured Application Logger ─────────────────────────────────────────

// SetupLogger configures zerolog for the process.
func SetupLogger(level, format string) {
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}
