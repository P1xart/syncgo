package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type SearchMetrics struct {
	requestsTotal      *prometheus.CounterVec
	errorsTotal        *prometheus.CounterVec
	flushRetriesTotal  prometheus.Counter
	flushFailuresTotal prometheus.Counter
}

func New() *SearchMetrics {
	searchMetrics := &SearchMetrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_search_requests_total",
				Help: "Total number of bulk requests sent to the search backend.",
			},
			[]string{"backend", "status"}, // status: "success" | "fail"
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_search_errors_total",
				Help: "Total number of indexing errors reported by the search backend.",
			},
			[]string{"backend"},
		),
		flushRetriesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "syncgo_batcher_flush_retries_total",
				Help: "Total number of batcher flush retry attempts.",
			},
		),
		flushFailuresTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "syncgo_batcher_flush_failures_total",
				Help: "Total number of batcher flushes that failed after exhausting all retries.",
			},
		),
	}

	prometheus.MustRegister(
		searchMetrics.requestsTotal,
		searchMetrics.errorsTotal,
		searchMetrics.flushRetriesTotal,
		searchMetrics.flushFailuresTotal,
	)

	return searchMetrics
}

// IncSearchRequests increments the request counter for the given backend and status.
// Status should be "success" (no errors for that request) or "fail".
func (m *SearchMetrics) IncSearchRequests(backend, status string) {
	m.requestsTotal.WithLabelValues(backend, status).Inc()
}

// AddSearchErrors adds the given count to the errors counter for the backend.
func (m *SearchMetrics) AddSearchErrors(backend string, count float64) {
	if count <= 0 {
		return
	}
	m.errorsTotal.WithLabelValues(backend).Add(count)
}

// IncFlushRetry increments the batcher flush retry counter.
func (m *SearchMetrics) IncFlushRetry() {
	m.flushRetriesTotal.Inc()
}

// IncFlushFailure increments the batcher flush failure counter.
func (m *SearchMetrics) IncFlushFailure() {
	m.flushFailuresTotal.Inc()
}
